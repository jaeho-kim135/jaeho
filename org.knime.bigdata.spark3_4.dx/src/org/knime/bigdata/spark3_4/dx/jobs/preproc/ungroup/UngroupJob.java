package org.knime.bigdata.spark3_4.dx.jobs.preproc.ungroup;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.ungroup.SparkUngroupJobInput;
import org.knime.bigdata.spark.dx.node.preproc.ungroup.SparkUngroupJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.explode_outer;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.trim;

/**
 * Spark job that performs the ungroup (explode) operation.
 * Supports Array and String column types with optional string splitting.
 * MapType columns are rejected with a clear error message.
 */
@SparkClass
public class UngroupJob implements SparkJob<SparkUngroupJobInput, SparkUngroupJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkUngroupJobOutput runJob(final SparkContext sparkContext, final SparkUngroupJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] srcCols = input.getColumns();
        final String explodeMode = input.getExplodeMode();
        final String delimiter = input.getDelimiter();
        final boolean removeOriginal = input.isRemoveOriginal();
        final boolean skipNulls = input.isSkipNulls();
        final boolean skipEmpty = input.isSkipEmpty();
        final boolean validateOnly = input.isValidateOnly();

        if (srcCols == null || srcCols.length == 0) {
            throw new KNIMESparkException("No target columns specified for ungrouping.");
        }

        Dataset<Row> result = inputFrame;

        for (String srcCol : srcCols) {
            final DataType colType = result.schema().apply(srcCol).dataType();
            String outputCol;
            Column explodeCol;

            if ("STRING_SPLIT".equals(explodeMode) || colType instanceof StringType) {
                // String split mode or auto-detected string column
                final Column splitResult = split(col("`" + srcCol + "`"), Pattern.quote(delimiter));
                explodeCol = skipNulls ? explode(splitResult) : explode_outer(splitResult);
            } else if (colType instanceof ArrayType) {
                explodeCol = skipNulls
                    ? explode(col("`" + srcCol + "`"))
                    : explode_outer(col("`" + srcCol + "`"));
            } else if (colType instanceof MapType) {
                throw new KNIMESparkException("Column '" + srcCol
                    + "' is a MapType column. MapType explode produces two columns (key and value) "
                    + "which is not supported by this node. Please convert the map column to an "
                    + "array first using a Spark SQL expression, e.g. map_keys() or map_values().");
            } else {
                throw new KNIMESparkException("Column '" + srcCol
                    + "' is not Array, Map, or String type. Actual type: " + colType.typeName());
            }

            // Apply explode
            if (removeOriginal) {
                // Select all columns except srcCol, then add explodeCol as srcCol
                final Column[] selectCols = Arrays.stream(result.columns())
                    .filter(c -> !c.equals(srcCol))
                    .map(c -> col("`" + c + "`"))
                    .toArray(Column[]::new);
                final Column[] allCols = Arrays.copyOf(selectCols, selectCols.length + 1);
                allCols[selectCols.length] = explodeCol.as(srcCol);
                result = result.select(allCols);
                outputCol = srcCol;
            } else {
                // Keep original + add exploded column with suffix
                final String explodedColName = srcCol + "_exploded";
                result = result.select(col("*"), explodeCol.as(explodedColName));
                outputCol = explodedColName;
            }

            // Trim for string split mode
            if ("STRING_SPLIT".equals(explodeMode)) {
                result = result.withColumn(outputCol, trim(col("`" + outputCol + "`")));
            }

            // Skip empty (independent of skipNulls)
            if (skipEmpty) {
                result = result.filter(
                    col("`" + outputCol + "`").isNull()
                        .or(col("`" + outputCol + "`").notEqual("")));
            }
        }

        if (validateOnly) {
            final SparkUngroupJobOutput output = new SparkUngroupJobOutput(null, null);
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
        return new SparkUngroupJobOutput(namedOutputObject, outputSchema);
    }
}
