package org.knime.bigdata.spark.dx.node.preproc.stringreplacer;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark String Replacer job.
 * Contains all configuration needed for find-and-replace operations.
 */
@SparkClass
public class SparkStringReplacerJobInput extends JobInput {

    private static final String KEY_COLUMN = "column";
    private static final String KEY_PATTERN_TYPE = "patternType";
    private static final String KEY_PATTERN = "pattern";
    private static final String KEY_REPLACEMENT = "replacement";
    private static final String KEY_CASE_SENSITIVE = "caseSensitive";
    private static final String KEY_ENABLE_ESCAPING = "enableEscaping";
    private static final String KEY_REPLACEMENT_STRATEGY = "replacementStrategy";
    private static final String KEY_APPEND_OR_REPLACE = "appendOrReplace";
    private static final String KEY_NEW_COL_NAME = "newColName";

    /** Deserialization constructor. */
    public SparkStringReplacerJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param column target column name
     * @param patternType pattern type (LITERAL, WILDCARD, REGEX)
     * @param pattern search pattern
     * @param replacement replacement text
     * @param caseSensitive whether matching is case-sensitive
     * @param enableEscaping whether wildcard escaping is enabled
     * @param replacementStrategy replacement strategy (ALL_OCCURRENCES, WHOLE_STRING)
     * @param appendOrReplace output mode (APPEND, REPLACE)
     * @param newColName new column name (for APPEND mode)
     */
    public SparkStringReplacerJobInput(final String inputObject, final String outputObject,
            final String column, final String patternType, final String pattern,
            final String replacement, final boolean caseSensitive, final boolean enableEscaping,
            final String replacementStrategy, final String appendOrReplace, final String newColName) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(KEY_COLUMN, column);
        set(KEY_PATTERN_TYPE, patternType);
        set(KEY_PATTERN, pattern);
        set(KEY_REPLACEMENT, replacement);
        set(KEY_CASE_SENSITIVE, caseSensitive);
        set(KEY_ENABLE_ESCAPING, enableEscaping);
        set(KEY_REPLACEMENT_STRATEGY, replacementStrategy);
        set(KEY_APPEND_OR_REPLACE, appendOrReplace);
        set(KEY_NEW_COL_NAME, newColName);
    }

    /** @return the target column name */
    public String getColumn() {
        return get(KEY_COLUMN);
    }

    /** @return the pattern type (LITERAL, WILDCARD, REGEX) */
    public String getPatternType() {
        return get(KEY_PATTERN_TYPE);
    }

    /** @return the search pattern */
    public String getPattern() {
        return get(KEY_PATTERN);
    }

    /** @return the replacement text */
    public String getReplacement() {
        return get(KEY_REPLACEMENT);
    }

    /** @return whether matching is case-sensitive */
    public boolean isCaseSensitive() {
        return get(KEY_CASE_SENSITIVE);
    }

    /** @return whether wildcard escaping is enabled */
    public boolean isEnableEscaping() {
        return get(KEY_ENABLE_ESCAPING);
    }

    /** @return the replacement strategy (ALL_OCCURRENCES, WHOLE_STRING) */
    public String getReplacementStrategy() {
        return get(KEY_REPLACEMENT_STRATEGY);
    }

    /** @return true if the output mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(get(KEY_APPEND_OR_REPLACE));
    }

    /** @return the new column name (for APPEND mode) */
    public String getNewColName() {
        return get(KEY_NEW_COL_NAME);
    }

    /**
     * Returns the effective output column name.
     * In REPLACE mode, returns the target column name.
     * In APPEND mode, returns the new column name.
     *
     * @return the effective output column name
     */
    public String getOutputColName() {
        return isReplace() ? getColumn() : getNewColName();
    }
}
