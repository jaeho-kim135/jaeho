package org.knime.bigdata.spark.dx.node.calculate.datetimediff;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Date&Time Difference job.
 * Contains first column, second mode, second column / fixed value, direction, granularity,
 * and output column name.
 */
@SparkClass
public class SparkDateTimeDifferenceJobInput extends JobInput {

    private static final String FIRST_COLUMN = "firstColumn";
    private static final String SECOND_MODE = "secondMode";
    private static final String SECOND_COLUMN = "secondColumn";
    private static final String FIXED_DATE_TIME = "fixedDateTime";
    private static final String DIRECTION = "direction";
    private static final String GRANULARITY = "granularity";
    private static final String OUTPUT_COL_NAME = "outputColName";

    /** Deserialization constructor. */
    public SparkDateTimeDifferenceJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param firstColumn the first date/time column name
     * @param secondMode the second value source mode (COLUMN, FIXED, CURRENT)
     * @param secondColumn the second column name (for COLUMN mode)
     * @param fixedDateTime the fixed date/time string (for FIXED mode)
     * @param direction the difference direction (SECOND_MINUS_FIRST or FIRST_MINUS_SECOND)
     * @param granularity the result granularity (YEAR, MONTH, ... MICROSECOND)
     * @param outputColName the output column name
     */
    public SparkDateTimeDifferenceJobInput(final String inputObject, final String outputObject,
            final String firstColumn, final String secondMode,
            final String secondColumn, final String fixedDateTime,
            final String direction, final String granularity,
            final String outputColName) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(FIRST_COLUMN, firstColumn);
        set(SECOND_MODE, secondMode);
        set(SECOND_COLUMN, secondColumn);
        set(FIXED_DATE_TIME, fixedDateTime);
        set(DIRECTION, direction);
        set(GRANULARITY, granularity);
        set(OUTPUT_COL_NAME, outputColName);
    }

    /** @return the first date/time column name */
    public String getFirstColumn() {
        return get(FIRST_COLUMN);
    }

    /** @return the second value source mode (COLUMN, FIXED, CURRENT) */
    public String getSecondMode() {
        return getOrDefault(SECOND_MODE, "COLUMN");
    }

    /** @return the second column name (for COLUMN mode) */
    public String getSecondColumn() {
        return getOrDefault(SECOND_COLUMN, "");
    }

    /** @return the fixed date/time string (for FIXED mode) */
    public String getFixedDateTime() {
        return getOrDefault(FIXED_DATE_TIME, "");
    }

    /** @return the difference direction (SECOND_MINUS_FIRST or FIRST_MINUS_SECOND) */
    public String getDirection() {
        return getOrDefault(DIRECTION, "SECOND_MINUS_FIRST");
    }

    /** @return the result granularity */
    public String getGranularity() {
        return getOrDefault(GRANULARITY, "DAY");
    }

    /** @return the output column name */
    public String getOutputColName() {
        return getOrDefault(OUTPUT_COL_NAME, "Difference");
    }
}
