package org.knime.bigdata.spark3_4.dx.jobs.extract.datetimefields;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.extract.datetimefields.SparkExtractDateTimeFieldsJobInput;
import org.knime.bigdata.spark.dx.node.extract.datetimefields.SparkExtractDateTimeFieldsJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.*;

/**
 * Spark 3.4 job that extracts date/time fields from a date/time column.
 * Uses Spark SQL functions such as {@code year()}, {@code month()}, {@code dayofmonth()},
 * {@code hour()}, {@code minute()}, {@code second()}, {@code date_format()}, etc.
 */
@SparkClass
public class ExtractDateTimeFieldsJob
    implements SparkJob<SparkExtractDateTimeFieldsJobInput, SparkExtractDateTimeFieldsJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkExtractDateTimeFieldsJobOutput runJob(final SparkContext sparkContext,
            final SparkExtractDateTimeFieldsJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String columnName = input.getColumn();
        final String safeColName = columnName.replace("`", "``");
        final Column srcCol = col("`" + safeColName + "`");
        String prefix = input.getColumnPrefix();
        if (prefix == null) {
            prefix = "";
        }

        Dataset<Row> result = inputFrame;

        // Date fields
        if (input.isExtractYear()) {
            result = result.withColumn(prefix + "Year", year(srcCol));
        }
        if (input.isExtractMonth()) {
            result = result.withColumn(prefix + "Month", month(srcCol));
        }
        if (input.isExtractDay()) {
            result = result.withColumn(prefix + "Day", dayofmonth(srcCol));
        }
        if (input.isExtractDayOfWeek()) {
            result = result.withColumn(prefix + "DayOfWeek", dayofweek(srcCol));
        }
        if (input.isExtractDayOfYear()) {
            result = result.withColumn(prefix + "DayOfYear", dayofyear(srcCol));
        }
        if (input.isExtractWeekOfYear()) {
            result = result.withColumn(prefix + "WeekOfYear", weekofyear(srcCol));
        }
        if (input.isExtractQuarter()) {
            result = result.withColumn(prefix + "Quarter", quarter(srcCol));
        }

        // Time fields
        if (input.isExtractHour()) {
            result = result.withColumn(prefix + "Hour", hour(srcCol));
        }
        if (input.isExtractMinute()) {
            result = result.withColumn(prefix + "Minute", minute(srcCol));
        }
        if (input.isExtractSecond()) {
            result = result.withColumn(prefix + "Second", second(srcCol));
        }

        // Subsecond
        if (input.isExtractSubsecond()) {
            final String unit = input.getSubsecondUnit();
            if ("MILLISECOND".equals(unit)) {
                result = result.withColumn(prefix + "Millisecond",
                    date_format(srcCol, "SSS").cast("int"));
            } else if ("MICROSECOND".equals(unit)) {
                result = result.withColumn(prefix + "Microsecond",
                    expr("CAST(unix_micros(`" + safeColName + "`) % 1000000 AS INT)"));
            } else { // NANOSECOND
                result = result.withColumn(prefix + "Nanosecond",
                    expr("CAST((unix_micros(`" + safeColName + "`) % 1000000) * 1000 AS INT)"));
            }
        }

        // Name fields (locale dependent)
        if (input.isExtractDayOfWeekName()) {
            final String locale = input.getLocale();
            if (locale != null && !locale.isEmpty()) {
                final String safeLocale = locale.replace("'", "''");
                // Use date_format with locale via SQL expression
                result = result.withColumn(prefix + "DayOfWeekName",
                    expr("date_format(`" + safeColName + "`, 'EEEE', '" + safeLocale + "')"));
            } else {
                result = result.withColumn(prefix + "DayOfWeekName",
                    date_format(srcCol, "EEEE"));
            }
        }
        if (input.isExtractMonthName()) {
            final String locale = input.getLocale();
            if (locale != null && !locale.isEmpty()) {
                final String safeLocale = locale.replace("'", "''");
                result = result.withColumn(prefix + "MonthName",
                    expr("date_format(`" + safeColName + "`, 'MMMM', '" + safeLocale + "')"));
            } else {
                result = result.withColumn(prefix + "MonthName",
                    date_format(srcCol, "MMMM"));
            }
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkExtractDateTimeFieldsJobOutput(namedOutputObject, outputSchema);
    }
}
