package org.knime.bigdata.spark3_4.dx.jobs.calculate.datetimediff;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.calculate.datetimediff.SparkDateTimeDifferenceJobInput;
import org.knime.bigdata.spark.dx.node.calculate.datetimediff.SparkDateTimeDifferenceJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.expr;

/**
 * Spark job that calculates the difference between two date/time values.
 * Supports column-to-column, column-to-fixed, and column-to-current-timestamp modes
 * with configurable granularity from years down to microseconds.
 */
@SparkClass
public class DateTimeDifferenceJob
    implements SparkJob<SparkDateTimeDifferenceJobInput, SparkDateTimeDifferenceJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkDateTimeDifferenceJobOutput runJob(final SparkContext sparkContext,
            final SparkDateTimeDifferenceJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputDF = namedObjects.getDataFrame(namedInputObject);

        // Build the column/expression references for first and second values
        final String col1 = "`" + input.getFirstColumn() + "`";
        final String col2;
        switch (input.getSecondMode()) {
            case "COLUMN":
                col2 = "`" + input.getSecondColumn() + "`";
                break;
            case "FIXED":
                col2 = "TIMESTAMP '" + input.getFixedDateTime() + "'";
                break;
            case "CURRENT":
                col2 = "current_timestamp()";
                break;
            default:
                col2 = "current_timestamp()";
                break;
        }

        // Determine start and end based on direction
        final String start;
        final String end;
        if ("SECOND_MINUS_FIRST".equals(input.getDirection())) {
            start = col1;
            end = col2;
        } else {
            start = col2;
            end = col1;
        }

        // Build the Spark SQL expression based on granularity
        final String sparkExpr;
        switch (input.getGranularity()) {
            case "YEAR":
                sparkExpr = "CAST(months_between(" + end + ", " + start + ") / 12 AS INT)";
                break;
            case "MONTH":
                sparkExpr = "CAST(months_between(" + end + ", " + start + ") AS INT)";
                break;
            case "WEEK":
                sparkExpr = "CAST(datediff(" + end + ", " + start + ") / 7 AS INT)";
                break;
            case "DAY":
                sparkExpr = "datediff(" + end + ", " + start + ")";
                break;
            case "HOUR":
                sparkExpr = "CAST((unix_timestamp(" + end + ") - unix_timestamp(" + start + ")) AS DOUBLE) / 3600.0";
                break;
            case "MINUTE":
                sparkExpr = "CAST((unix_timestamp(" + end + ") - unix_timestamp(" + start + ")) AS DOUBLE) / 60.0";
                break;
            case "SECOND":
                sparkExpr = "CAST(unix_timestamp(" + end + ") - unix_timestamp(" + start + ") AS LONG)";
                break;
            case "MILLISECOND":
                sparkExpr = "CAST(unix_millis(CAST(" + end + " AS TIMESTAMP)) - unix_millis(CAST(" + start + " AS TIMESTAMP)) AS LONG)";
                break;
            case "MICROSECOND":
                sparkExpr = "CAST(unix_micros(CAST(" + end + " AS TIMESTAMP)) - unix_micros(CAST(" + start + " AS TIMESTAMP)) AS LONG)";
                break;
            default:
                throw new KNIMESparkException("Unsupported granularity: " + input.getGranularity());
        }

        final Dataset<Row> result = inputDF.withColumn(input.getOutputColName(), expr(sparkExpr));

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkDateTimeDifferenceJobOutput(namedOutputObject, outputSchema);
    }
}
