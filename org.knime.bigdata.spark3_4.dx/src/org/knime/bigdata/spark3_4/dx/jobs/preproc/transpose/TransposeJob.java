package org.knime.bigdata.spark3_4.dx.jobs.preproc.transpose;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.transpose.SparkTransposeJobInput;
import org.knime.bigdata.spark.dx.node.preproc.transpose.SparkTransposeJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

/**
 * Spark job that transposes a DataFrame by collecting all data to the driver
 * and reconstructing it with rows and columns swapped. All values are converted
 * to String type in the transposed output.
 *
 * <p><b>WARNING:</b> This job collects all data to the driver.
 * Only suitable for small datasets.</p>
 */
@SparkClass
public class TransposeJob implements SparkJob<SparkTransposeJobInput, SparkTransposeJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkTransposeJobOutput runJob(final SparkContext sparkContext, final SparkTransposeJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputDF = namedObjects.getDataFrame(namedInputObject);
        final int maxRows = input.getMaxRows();
        final String idColumn = input.getIdColumn();
        final boolean validateOnly = input.isValidateOnly();

        // 1. Check row count
        final long rowCount = inputDF.count();
        if (rowCount > maxRows) {
            throw new KNIMESparkException(
                "Input has " + rowCount + " rows which exceeds the maximum limit of " + maxRows + ". "
                + "Increase the 'Maximum rows' setting or reduce the input data size.");
        }

        // 2. Collect to driver
        final List<Row> rows = inputDF.collectAsList();
        final String[] colNames = inputDF.columns();

        // 3. Determine row IDs (new column names)
        int idColIdx = -1;
        if (idColumn != null && !idColumn.isEmpty()) {
            idColIdx = Arrays.asList(colNames).indexOf(idColumn);
            if (idColIdx < 0) {
                throw new KNIMESparkException("ID column '" + idColumn + "' not found in input DataFrame.");
            }
        }

        final List<String> newColNames = new ArrayList<>();
        final Set<String> usedNames = new HashSet<>();
        for (int r = 0; r < rows.size(); r++) {
            String name;
            if (idColIdx >= 0) {
                final Object val = rows.get(r).get(idColIdx);
                name = val != null ? val.toString() : "null_" + r;
            } else {
                name = "Row" + r;
            }
            // Deduplicate column names to avoid AnalysisException
            if (usedNames.contains(name)) {
                int suffix = 1;
                while (usedNames.contains(name + "_" + suffix)) {
                    suffix++;
                }
                name = name + "_" + suffix;
            }
            usedNames.add(name);
            newColNames.add(name);
        }

        // 4. Transpose: original columns become rows, original rows become columns
        final List<String> dataCols = new ArrayList<>(Arrays.asList(colNames));
        if (idColIdx >= 0) {
            dataCols.remove(idColIdx);
        }

        final List<Row> transposedRows = new ArrayList<>();
        for (final String origCol : dataCols) {
            final int colIdx = Arrays.asList(colNames).indexOf(origCol);
            final Object[] values = new Object[rows.size() + 1];
            values[0] = origCol;  // first column = original column name
            for (int r = 0; r < rows.size(); r++) {
                final Object rawVal = rows.get(r).get(colIdx);
                values[r + 1] = rawVal != null ? rawVal.toString() : null;
            }
            transposedRows.add(RowFactory.create(values));
        }

        // 5. Build schema
        final List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("Column", DataTypes.StringType, false));
        for (final String newCol : newColNames) {
            fields.add(DataTypes.createStructField(newCol, DataTypes.StringType, true));
        }
        final StructType schema = DataTypes.createStructType(fields);

        // 6. Create DataFrame
        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final Dataset<Row> result = spark.createDataFrame(transposedRows, schema);

        if (validateOnly) {
            final SparkTransposeJobOutput output = new SparkTransposeJobOutput(null, null);
            try {
                final String preview = result.showString(5, 20, false);
                output.setPreviewData(preview);
            } catch (final Exception e) {
                output.setPreviewData("Preview failed: " + e.getMessage());
            }
            return output;
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkTransposeJobOutput(namedOutputObject, outputSchema);
    }
}
