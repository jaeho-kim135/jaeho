package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Rule Engine job.
 * Contains rule lines, default value settings, and output column configuration.
 */
@SparkClass
public class SparkRuleEngineJobInput extends JobInput {

    private static final String RULES = "rules";
    private static final String DEFAULT_VALUE = "defaultValue";
    private static final String DEFAULT_IS_MISSING = "defaultIsMissing";
    private static final String IS_REPLACE = "isReplace";
    private static final String OUTPUT_COLUMN = "outputColumn";
    private static final String VALIDATE_ONLY = "validateOnly";

    /** Deserialization constructor. */
    public SparkRuleEngineJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param rules rule lines (one rule per array element)
     * @param defaultValue default value for non-matching rows
     * @param defaultIsMissing true if default should be null
     * @param isReplace true to replace an existing column
     * @param outputColumn name of the output/replace column
     */
    public SparkRuleEngineJobInput(final String inputObject, final String outputObject,
            final String[] rules, final String defaultValue, final boolean defaultIsMissing,
            final boolean isReplace, final String outputColumn) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(RULES, rules);
        set(DEFAULT_VALUE, defaultValue);
        set(DEFAULT_IS_MISSING, defaultIsMissing);
        set(IS_REPLACE, isReplace);
        set(OUTPUT_COLUMN, outputColumn);
        set(VALIDATE_ONLY, false);
    }

    /**
     * Constructor for validation-only execution.
     *
     * @param inputObject named input object ID
     * @param rules rule lines
     * @param defaultValue default value
     * @param defaultIsMissing true if default should be null
     * @param isReplace true to replace an existing column
     * @param outputColumn name of the output column
     */
    public SparkRuleEngineJobInput(final String inputObject,
            final String[] rules, final String defaultValue, final boolean defaultIsMissing,
            final boolean isReplace, final String outputColumn) {
        addNamedInputObject(inputObject);
        set(RULES, rules);
        set(DEFAULT_VALUE, defaultValue);
        set(DEFAULT_IS_MISSING, defaultIsMissing);
        set(IS_REPLACE, isReplace);
        set(OUTPUT_COLUMN, outputColumn);
        set(VALIDATE_ONLY, true);
    }

    /** @return the rule lines */
    public String[] getRules() {
        return get(RULES);
    }

    /** @return the default value for non-matching rows */
    public String getDefaultValue() {
        return getOrDefault(DEFAULT_VALUE, "");
    }

    /** @return true if the default value should be null */
    public boolean isDefaultMissing() {
        return get(DEFAULT_IS_MISSING);
    }

    /** @return true if the output should replace an existing column */
    public boolean isReplace() {
        return get(IS_REPLACE);
    }

    /** @return the output column name */
    public String getOutputColumn() {
        return get(OUTPUT_COLUMN);
    }

    /** @return whether this is a validation-only run */
    public boolean isValidateOnly() {
        return get(VALIDATE_ONLY);
    }
}
