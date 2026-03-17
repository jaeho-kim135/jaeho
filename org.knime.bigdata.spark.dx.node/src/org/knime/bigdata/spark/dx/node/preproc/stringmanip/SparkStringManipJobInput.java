package org.knime.bigdata.spark.dx.node.preproc.stringmanip;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark String Manipulation job.
 * Contains a single expression, output mode, and column name settings.
 */
@SparkClass
public class SparkStringManipJobInput extends JobInput {

    private static final String EXPRESSION = "expression";
    private static final String APPEND_OR_REPLACE = "appendOrReplace";
    private static final String OUTPUT_COL_NAME = "outputColName";
    private static final String REPLACE_COLUMN = "replaceColumn";
    private static final String VALIDATE_ONLY = "validateOnly";

    /** Deserialization constructor. */
    public SparkStringManipJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param expression Spark SQL string expression
     * @param appendOrReplace output mode ("APPEND" or "REPLACE")
     * @param outputColName output column name (for APPEND mode)
     * @param replaceColumn column to replace (for REPLACE mode)
     */
    public SparkStringManipJobInput(final String inputObject, final String outputObject,
            final String expression, final String appendOrReplace,
            final String outputColName, final String replaceColumn) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(EXPRESSION, expression);
        set(APPEND_OR_REPLACE, appendOrReplace);
        set(OUTPUT_COL_NAME, outputColName);
        set(REPLACE_COLUMN, replaceColumn);
        set(VALIDATE_ONLY, false);
    }

    /**
     * Constructor for validation-only execution.
     *
     * @param inputObject named input object ID
     * @param expression Spark SQL string expression
     * @param appendOrReplace output mode ("APPEND" or "REPLACE")
     * @param outputColName output column name (for APPEND mode)
     * @param replaceColumn column to replace (for REPLACE mode)
     */
    public SparkStringManipJobInput(final String inputObject,
            final String expression, final String appendOrReplace,
            final String outputColName, final String replaceColumn) {
        addNamedInputObject(inputObject);
        set(EXPRESSION, expression);
        set(APPEND_OR_REPLACE, appendOrReplace);
        set(OUTPUT_COL_NAME, outputColName);
        set(REPLACE_COLUMN, replaceColumn);
        set(VALIDATE_ONLY, true);
    }

    /** @return the Spark SQL string expression */
    public String getExpression() {
        return get(EXPRESSION);
    }

    /** @return the output mode ("APPEND" or "REPLACE") */
    public String getAppendOrReplace() {
        return get(APPEND_OR_REPLACE);
    }

    /** @return true if the output mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(getAppendOrReplace());
    }

    /** @return the output column name (for APPEND mode) */
    public String getOutputColName() {
        return get(OUTPUT_COL_NAME);
    }

    /** @return the column to replace (for REPLACE mode) */
    public String getReplaceColumn() {
        return get(REPLACE_COLUMN);
    }

    /** @return whether this is a validation-only run */
    public boolean isValidateOnly() {
        return get(VALIDATE_ONLY);
    }
}
