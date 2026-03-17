package org.knime.bigdata.spark.dx.node.extract.datetimefields;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Extract Date&amp;Time Fields job.
 * Contains the source column, flags for each extractable field, locale, and output prefix.
 */
@SparkClass
public class SparkExtractDateTimeFieldsJobInput extends JobInput {

    private static final String COLUMN = "column";
    private static final String EXTRACT_YEAR = "extractYear";
    private static final String EXTRACT_MONTH = "extractMonth";
    private static final String EXTRACT_DAY = "extractDay";
    private static final String EXTRACT_HOUR = "extractHour";
    private static final String EXTRACT_MINUTE = "extractMinute";
    private static final String EXTRACT_SECOND = "extractSecond";
    private static final String EXTRACT_DAY_OF_WEEK = "extractDayOfWeek";
    private static final String EXTRACT_DAY_OF_YEAR = "extractDayOfYear";
    private static final String EXTRACT_WEEK_OF_YEAR = "extractWeekOfYear";
    private static final String EXTRACT_QUARTER = "extractQuarter";
    private static final String EXTRACT_SUBSECOND = "extractSubsecond";
    private static final String SUBSECOND_UNIT = "subsecondUnit";
    private static final String EXTRACT_DAY_OF_WEEK_NAME = "extractDayOfWeekName";
    private static final String EXTRACT_MONTH_NAME = "extractMonthName";
    private static final String LOCALE = "locale";
    private static final String COLUMN_PREFIX = "columnPrefix";

    /** Deserialization constructor. */
    public SparkExtractDateTimeFieldsJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param column the source date/time column
     * @param extractYear whether to extract year
     * @param extractMonth whether to extract month
     * @param extractDay whether to extract day of month
     * @param extractHour whether to extract hour
     * @param extractMinute whether to extract minute
     * @param extractSecond whether to extract second
     * @param extractDayOfWeek whether to extract day of week (number)
     * @param extractDayOfYear whether to extract day of year
     * @param extractWeekOfYear whether to extract week of year
     * @param extractQuarter whether to extract quarter
     * @param extractSubsecond whether to extract subsecond
     * @param subsecondUnit the subsecond unit (MILLISECOND, MICROSECOND, NANOSECOND)
     * @param extractDayOfWeekName whether to extract day of week name
     * @param extractMonthName whether to extract month name
     * @param locale the locale for name fields
     * @param columnPrefix the prefix for output column names
     */
    public SparkExtractDateTimeFieldsJobInput(final String inputObject, final String outputObject,
            final String column,
            final boolean extractYear, final boolean extractMonth, final boolean extractDay,
            final boolean extractHour, final boolean extractMinute, final boolean extractSecond,
            final boolean extractDayOfWeek, final boolean extractDayOfYear,
            final boolean extractWeekOfYear, final boolean extractQuarter,
            final boolean extractSubsecond, final String subsecondUnit,
            final boolean extractDayOfWeekName, final boolean extractMonthName,
            final String locale, final String columnPrefix) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMN, column);
        set(EXTRACT_YEAR, extractYear);
        set(EXTRACT_MONTH, extractMonth);
        set(EXTRACT_DAY, extractDay);
        set(EXTRACT_HOUR, extractHour);
        set(EXTRACT_MINUTE, extractMinute);
        set(EXTRACT_SECOND, extractSecond);
        set(EXTRACT_DAY_OF_WEEK, extractDayOfWeek);
        set(EXTRACT_DAY_OF_YEAR, extractDayOfYear);
        set(EXTRACT_WEEK_OF_YEAR, extractWeekOfYear);
        set(EXTRACT_QUARTER, extractQuarter);
        set(EXTRACT_SUBSECOND, extractSubsecond);
        set(SUBSECOND_UNIT, subsecondUnit);
        set(EXTRACT_DAY_OF_WEEK_NAME, extractDayOfWeekName);
        set(EXTRACT_MONTH_NAME, extractMonthName);
        set(LOCALE, locale);
        set(COLUMN_PREFIX, columnPrefix);
    }

    /** @return the source date/time column name */
    public String getColumn() { return get(COLUMN); }

    /** @return true if Year should be extracted */
    public boolean isExtractYear() { return getOrDefault(EXTRACT_YEAR, false); }

    /** @return true if Month should be extracted */
    public boolean isExtractMonth() { return getOrDefault(EXTRACT_MONTH, false); }

    /** @return true if Day of month should be extracted */
    public boolean isExtractDay() { return getOrDefault(EXTRACT_DAY, false); }

    /** @return true if Hour should be extracted */
    public boolean isExtractHour() { return getOrDefault(EXTRACT_HOUR, false); }

    /** @return true if Minute should be extracted */
    public boolean isExtractMinute() { return getOrDefault(EXTRACT_MINUTE, false); }

    /** @return true if Second should be extracted */
    public boolean isExtractSecond() { return getOrDefault(EXTRACT_SECOND, false); }

    /** @return true if Day of week (number) should be extracted */
    public boolean isExtractDayOfWeek() { return getOrDefault(EXTRACT_DAY_OF_WEEK, false); }

    /** @return true if Day of year should be extracted */
    public boolean isExtractDayOfYear() { return getOrDefault(EXTRACT_DAY_OF_YEAR, false); }

    /** @return true if Week of year should be extracted */
    public boolean isExtractWeekOfYear() { return getOrDefault(EXTRACT_WEEK_OF_YEAR, false); }

    /** @return true if Quarter should be extracted */
    public boolean isExtractQuarter() { return getOrDefault(EXTRACT_QUARTER, false); }

    /** @return true if Subsecond field should be extracted */
    public boolean isExtractSubsecond() { return getOrDefault(EXTRACT_SUBSECOND, false); }

    /** @return the subsecond unit (MILLISECOND, MICROSECOND, NANOSECOND) */
    public String getSubsecondUnit() { return getOrDefault(SUBSECOND_UNIT, "MILLISECOND"); }

    /** @return true if Day of week name should be extracted */
    public boolean isExtractDayOfWeekName() { return getOrDefault(EXTRACT_DAY_OF_WEEK_NAME, false); }

    /** @return true if Month name should be extracted */
    public boolean isExtractMonthName() { return getOrDefault(EXTRACT_MONTH_NAME, false); }

    /** @return the locale for name fields */
    public String getLocale() { return getOrDefault(LOCALE, "en"); }

    /** @return the column name prefix */
    public String getColumnPrefix() { return getOrDefault(COLUMN_PREFIX, ""); }
}
