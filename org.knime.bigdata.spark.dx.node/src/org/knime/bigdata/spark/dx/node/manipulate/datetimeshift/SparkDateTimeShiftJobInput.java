package org.knime.bigdata.spark.dx.node.manipulate.datetimeshift;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Date&amp;Time Shift job.
 * Contains the selected columns, shift mode, shift value/column, granularity,
 * replace flag, and suffix.
 */
@SparkClass
public class SparkDateTimeShiftJobInput extends JobInput {

    private static final String KEY_COLUMNS = "columns";
    private static final String KEY_SHIFT_MODE = "shiftMode";
    private static final String KEY_SHIFT_VALUE = "shiftValue";
    private static final String KEY_SHIFT_COLUMN = "shiftColumn";
    private static final String KEY_GRANULARITY = "granularity";
    private static final String KEY_IS_REPLACE = "isReplace";
    private static final String KEY_SUFFIX = "suffix";

    /** Deserialization constructor. */
    public SparkDateTimeShiftJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param columns the date/time columns to shift
     * @param shiftMode FIXED or COLUMN
     * @param shiftValue the fixed shift amount (used in FIXED mode)
     * @param shiftColumn the column containing shift values (used in COLUMN mode)
     * @param granularity the time unit (YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND)
     * @param isReplace true to replace columns, false to append
     * @param suffix the suffix for appended columns (used in APPEND mode)
     */
    public SparkDateTimeShiftJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String shiftMode, final int shiftValue,
            final String shiftColumn, final String granularity,
            final boolean isReplace, final String suffix) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(KEY_COLUMNS, columns);
        set(KEY_SHIFT_MODE, shiftMode);
        set(KEY_SHIFT_VALUE, shiftValue);
        set(KEY_SHIFT_COLUMN, shiftColumn);
        set(KEY_GRANULARITY, granularity);
        set(KEY_IS_REPLACE, isReplace);
        set(KEY_SUFFIX, suffix);
    }

    /** @return the date/time columns to shift */
    public String[] getColumns() {
        return get(KEY_COLUMNS);
    }

    /** @return the shift mode (FIXED or COLUMN) */
    public String getShiftMode() {
        return get(KEY_SHIFT_MODE);
    }

    /** @return the fixed shift amount */
    public int getShiftValue() {
        return getInteger(KEY_SHIFT_VALUE);
    }

    /** @return the column containing shift values */
    public String getShiftColumn() {
        return get(KEY_SHIFT_COLUMN);
    }

    /** @return the granularity (YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND) */
    public String getGranularity() {
        return get(KEY_GRANULARITY);
    }

    /** @return true to replace columns, false to append */
    public boolean isReplace() {
        return get(KEY_IS_REPLACE);
    }

    /** @return the suffix for appended columns */
    public String getSuffix() {
        return get(KEY_SUFFIX);
    }
}
