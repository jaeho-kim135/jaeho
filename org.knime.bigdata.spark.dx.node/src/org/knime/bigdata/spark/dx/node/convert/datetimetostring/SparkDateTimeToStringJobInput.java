package org.knime.bigdata.spark.dx.node.convert.datetimetostring;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Date&amp;Time to String job.
 * Contains column names, format pattern, locale, replace flag, and suffix.
 */
@SparkClass
public class SparkDateTimeToStringJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String FORMAT = "format";
    private static final String LOCALE = "locale";
    private static final String IS_REPLACE = "isReplace";
    private static final String SUFFIX = "suffix";

    /** Deserialization constructor. */
    public SparkDateTimeToStringJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param columns the date/time column names to convert
     * @param format the date format pattern
     * @param locale the locale string (e.g., "en", "ko")
     * @param isReplace true to replace selected columns, false to append new columns
     * @param suffix the suffix for new column names in append mode
     */
    public SparkDateTimeToStringJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String format, final String locale,
            final boolean isReplace, final String suffix) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(FORMAT, format);
        set(LOCALE, locale);
        set(IS_REPLACE, isReplace);
        set(SUFFIX, suffix);
    }

    /** @return the date/time column names to convert */
    public String[] getColumns() {
        return get(COLUMNS);
    }

    /** @return the date format pattern */
    public String getFormat() {
        return get(FORMAT);
    }

    /** @return the locale string */
    public String getLocale() {
        return getOrDefault(LOCALE, "en");
    }

    /** @return true if columns should be replaced, false if new columns should be appended */
    public boolean isReplace() {
        return get(IS_REPLACE);
    }

    /** @return the suffix for new column names in append mode */
    public String getSuffix() {
        return getOrDefault(SUFFIX, " (String)");
    }
}
