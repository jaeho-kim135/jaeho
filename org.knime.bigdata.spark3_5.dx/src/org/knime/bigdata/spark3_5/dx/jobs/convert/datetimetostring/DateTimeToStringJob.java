package org.knime.bigdata.spark3_5.dx.jobs.convert.datetimetostring;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.convert.datetimetostring.SparkDateTimeToStringJobInput;
import org.knime.bigdata.spark.dx.node.convert.datetimetostring.SparkDateTimeToStringJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * Spark job that converts date/time columns to string columns using
 * Spark's {@code date_format()} function.
 */
@SparkClass
public class DateTimeToStringJob
    implements SparkJob<SparkDateTimeToStringJobInput, SparkDateTimeToStringJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkDateTimeToStringJobOutput runJob(final SparkContext sparkContext,
            final SparkDateTimeToStringJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();
        final String format = input.getFormat();
        final boolean isReplace = input.isReplace();
        final String suffix = input.getSuffix();

        Dataset<Row> result = inputFrame;

        for (final String colName : columns) {
            final String outputCol = isReplace ? colName : colName + suffix;
            try {
                final String safeCol = colName.replace("`", "``");
                result = result.withColumn(outputCol,
                    date_format(col("`" + safeCol + "`"), format));
            } catch (final Exception e) {
                throw new KNIMESparkException(
                    "Failed to convert column '" + colName + "' with format '" + format
                    + "': " + e.getMessage(), e);
            }
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkDateTimeToStringJobOutput(namedOutputObject, outputSchema);
    }
}
