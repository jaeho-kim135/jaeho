package org.knime.bigdata.spark3_4.dx.jobs.extract.datetimefields;

import java.time.DayOfWeek;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.Locale;

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

        // Name fields (locale dependent) — uses element_at + array mapping
        // to avoid 3-arg date_format which is not supported in all Spark versions
        if (input.isExtractDayOfWeekName()) {
            final Locale javaLocale = toJavaLocale(input.getLocale());
            // Spark dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
            final String[] dayNames = new String[7];
            for (int i = 1; i <= 7; i++) {
                final int javaDow = (i == 1) ? 7 : i - 1;
                dayNames[i - 1] = DayOfWeek.of(javaDow)
                    .getDisplayName(TextStyle.FULL, javaLocale).replace("'", "''");
            }
            result = result.withColumn(prefix + "DayOfWeekName",
                expr(buildElementAtExpr(dayNames, "dayofweek(`" + safeColName + "`)")));
        }
        if (input.isExtractMonthName()) {
            final Locale javaLocale = toJavaLocale(input.getLocale());
            final String[] monthNames = new String[12];
            for (int i = 1; i <= 12; i++) {
                monthNames[i - 1] = Month.of(i)
                    .getDisplayName(TextStyle.FULL, javaLocale).replace("'", "''");
            }
            result = result.withColumn(prefix + "MonthName",
                expr(buildElementAtExpr(monthNames, "month(`" + safeColName + "`)")));
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkExtractDateTimeFieldsJobOutput(namedOutputObject, outputSchema);
    }

    private static Locale toJavaLocale(final String localeTag) {
        if (localeTag == null || localeTag.isEmpty()) {
            return Locale.ENGLISH;
        }
        return Locale.forLanguageTag(localeTag);
    }

    private static String buildElementAtExpr(final String[] names, final String indexExpr) {
        final StringBuilder sb = new StringBuilder("element_at(array(");
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("'").append(names[i]).append("'");
        }
        sb.append("), ").append(indexExpr).append(")");
        return sb.toString();
    }
}
