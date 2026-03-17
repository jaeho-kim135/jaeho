package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringtodatetime;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.stringtodatetime.SparkStringToDateTimeJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringtodatetime.SparkStringToDateTimeJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.trim;

/**
 * Spark job that converts String columns to date/time types using DataFrame API.
 */
@SparkClass
public class StringToDateTimeJob implements SparkJob<SparkStringToDateTimeJobInput, SparkStringToDateTimeJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkStringToDateTimeJobOutput runJob(final SparkContext sparkContext,
            final SparkStringToDateTimeJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();
        final String format = input.getFormat();
        final String outputType = input.getOutputType();
        final boolean failOnError = input.isFailOnError();

        Dataset<Row> result = inputFrame;

        for (final String colName : columns) {
            // Convert string to date/time based on output type
            final Column converted;
            switch (outputType) {
                case "DATE":
                    converted = to_date(col(colName), format);
                    break;
                case "TIME":
                    // Spark has no native time type; use to_timestamp.
                    // For time-only formats, Spark defaults missing date fields to epoch date (1970-01-01).
                    converted = to_timestamp(col(colName), format);
                    break;
                case "DATE_TIME":
                case "ZONED_DATE_TIME":
                default:
                    converted = to_timestamp(col(colName), format);
                    break;
            }

            if (failOnError) {
                // Save original value for error detection
                final String origCol = "_stdt_orig_" + colName;
                result = result.withColumn(origCol, col(colName));

                // Apply conversion
                result = result.withColumn(colName, converted);

                // Check for failures: original was non-null/non-blank but result is null
                final long failCount = result
                    .filter(
                        trim(col(origCol)).isNotNull()
                        .and(trim(col(origCol)).notEqual(lit("")))
                        .and(col(colName).isNull())
                    )
                    .count();

                if (failCount > 0) {
                    // Collect sample failing values
                    final List<Row> failingSamples = result
                        .filter(
                            trim(col(origCol)).isNotNull()
                            .and(trim(col(origCol)).notEqual(lit("")))
                            .and(col(colName).isNull())
                        )
                        .select(col(origCol))
                        .limit(3)
                        .collectAsList();

                    final List<String> sampleValues = new ArrayList<>();
                    for (final Row r : failingSamples) {
                        sampleValues.add(r.isNullAt(0) ? "null" : "\"" + r.getString(0) + "\"");
                    }

                    throw new KNIMESparkException("Failed to parse " + failCount
                        + " rows in column '" + colName + "' with format '" + format
                        + "'. Sample failing values: " + String.join(", ", sampleValues));
                }

                // Remove temp column
                result = result.drop(origCol);
            } else {
                // Simply replace the column (null for unparseable values)
                result = result.withColumn(colName, converted);
            }
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkStringToDateTimeJobOutput(namedOutputObject, outputSchema);
    }
}
