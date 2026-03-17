package org.knime.bigdata.spark3_5.dx.jobs.manipulate.datetimeshift;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.manipulate.datetimeshift.SparkDateTimeShiftJobInput;
import org.knime.bigdata.spark.dx.node.manipulate.datetimeshift.SparkDateTimeShiftJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.expr;

/**
 * Spark job that shifts date/time columns by a fixed or column-based value.
 * Uses Spark SQL functions: DATE_ADD, ADD_MONTHS, and MAKE_INTERVAL.
 */
@SparkClass
public class DateTimeShiftJob
    implements SparkJob<SparkDateTimeShiftJobInput, SparkDateTimeShiftJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkDateTimeShiftJobOutput runJob(final SparkContext sparkContext,
            final SparkDateTimeShiftJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();
        final String shiftMode = input.getShiftMode();
        final int shiftValue = input.getShiftValue();
        final String shiftColumn = input.getShiftColumn();
        final String granularity = input.getGranularity();
        final boolean isReplace = input.isReplace();
        final String suffix = input.getSuffix();

        Dataset<Row> result = inputFrame;

        for (final String colName : columns) {
            // Build the shift value expression
            final String shiftVal;
            if ("FIXED".equals(shiftMode)) {
                shiftVal = String.valueOf(shiftValue);
            } else {
                shiftVal = "CAST(`" + shiftColumn + "` AS INT)";
            }

            // Build the Spark SQL expression based on granularity
            final String sparkExpr;
            switch (granularity) {
                case "YEAR":
                    sparkExpr = "add_months(`" + colName + "`, " + shiftVal + " * 12)";
                    break;
                case "MONTH":
                    sparkExpr = "add_months(`" + colName + "`, " + shiftVal + ")";
                    break;
                case "WEEK":
                    sparkExpr = "date_add(`" + colName + "`, " + shiftVal + " * 7)";
                    break;
                case "DAY":
                    sparkExpr = "date_add(`" + colName + "`, " + shiftVal + ")";
                    break;
                case "HOUR":
                    sparkExpr = "(`" + colName + "` + make_interval(0, 0, 0, 0, " + shiftVal + ", 0, 0))";
                    break;
                case "MINUTE":
                    sparkExpr = "(`" + colName + "` + make_interval(0, 0, 0, 0, 0, " + shiftVal + ", 0))";
                    break;
                case "SECOND":
                    sparkExpr = "(`" + colName + "` + make_interval(0, 0, 0, 0, 0, 0, " + shiftVal + "))";
                    break;
                case "MILLISECOND":
                    sparkExpr = "(`" + colName + "` + make_interval(0, 0, 0, 0, 0, 0, " + shiftVal + " * 0.001))";
                    break;
                default:
                    throw new KNIMESparkException("Unsupported granularity: " + granularity);
            }

            // Determine the output column name
            final String outputCol;
            if (isReplace) {
                outputCol = colName;
            } else {
                outputCol = colName + suffix;
            }

            result = result.withColumn(outputCol, expr(sparkExpr));
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkDateTimeShiftJobOutput(namedOutputObject, outputSchema);
    }
}
