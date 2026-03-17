package org.knime.bigdata.spark.dx.node.preproc.rounddouble;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Number Rounder job.
 * Contains column names, number mode, precision, rounding method, and output mode.
 */
@SparkClass
public class SparkRoundDoubleJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String NUMBER_MODE = "numberMode";
    private static final String PRECISION = "precision";
    private static final String ROUNDING_STANDARD = "roundingStandard";
    private static final String ROUNDING_ADVANCED = "roundingAdvanced";
    private static final String IS_REPLACE = "isReplace";
    private static final String SUFFIX = "suffix";

    /** Deserialization constructor. */
    public SparkRoundDoubleJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param columns the columns to round
     * @param numberMode the number mode (DECIMALS, SIGNIFICANT_DIGITS, INTEGER)
     * @param precision the precision value
     * @param roundingStandard the rounding standard (HALF_AWAY_FROM_ZERO or OTHER)
     * @param roundingAdvanced the advanced rounding method
     * @param isReplace whether to replace or append
     * @param suffix the suffix for appended columns
     */
    public SparkRoundDoubleJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String numberMode, final int precision,
            final String roundingStandard, final String roundingAdvanced,
            final boolean isReplace, final String suffix) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(NUMBER_MODE, numberMode);
        set(PRECISION, precision);
        set(ROUNDING_STANDARD, roundingStandard);
        set(ROUNDING_ADVANCED, roundingAdvanced);
        set(IS_REPLACE, isReplace);
        set(SUFFIX, suffix);
    }

    /** @return the columns to round */
    public String[] getColumns() {
        return get(COLUMNS);
    }

    /** @return the number mode (DECIMALS, SIGNIFICANT_DIGITS, INTEGER) */
    public String getNumberMode() {
        return get(NUMBER_MODE);
    }

    /** @return the precision value */
    public int getPrecision() {
        return getInteger(PRECISION);
    }

    /** @return the rounding standard (HALF_AWAY_FROM_ZERO or OTHER) */
    public String getRoundingStandard() {
        return get(ROUNDING_STANDARD);
    }

    /** @return the advanced rounding method */
    public String getRoundingAdvanced() {
        return get(ROUNDING_ADVANCED);
    }

    /** @return true if columns should be replaced */
    public boolean isReplace() {
        return get(IS_REPLACE);
    }

    /** @return the suffix for appended columns */
    public String getSuffix() {
        return getOrDefault(SUFFIX, " (Rounded)");
    }
}
