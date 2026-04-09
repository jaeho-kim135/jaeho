package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Concatenate job.
 * Contains column mapping arrays and unmatched column handling actions.
 * Supports 2 named input objects (Left + Right DataFrames).
 */
@SparkClass
public class SparkConcatenateJobInput extends JobInput {

    private static final String KEY_LEFT_COLUMNS = "leftColumns";
    private static final String KEY_RIGHT_COLUMNS = "rightColumns";
    private static final String KEY_UNMATCHED_LEFT = "unmatchedLeftAction";
    private static final String KEY_UNMATCHED_RIGHT = "unmatchedRightAction";
    private static final String KEY_LEFT_INPUT = "leftInputObject";
    private static final String KEY_RIGHT_INPUT = "rightInputObject";

    /** Deserialization constructor. */
    public SparkConcatenateJobInput() {
    }

    /**
     * Constructor.
     *
     * @param leftInputObject named input object ID for the left DataFrame
     * @param rightInputObject named input object ID for the right DataFrame
     * @param outputObject named output object ID
     * @param leftColumns the left column names in the mapping (parallel array with rightColumns)
     * @param rightColumns the right column names in the mapping (empty = same name auto-mapping)
     * @param unmatchedLeftAction FILL_NULL or EXCLUDE
     * @param unmatchedRightAction FILL_NULL or EXCLUDE
     */
    public SparkConcatenateJobInput(final String leftInputObject, final String rightInputObject,
            final String outputObject,
            final String[] leftColumns, final String[] rightColumns,
            final String unmatchedLeftAction, final String unmatchedRightAction) {
        addNamedInputObject(leftInputObject);
        addNamedInputObject(rightInputObject);
        addNamedOutputObject(outputObject);
        set(KEY_LEFT_INPUT, leftInputObject);
        set(KEY_RIGHT_INPUT, rightInputObject);
        set(KEY_LEFT_COLUMNS, leftColumns);
        set(KEY_RIGHT_COLUMNS, rightColumns);
        set(KEY_UNMATCHED_LEFT, unmatchedLeftAction);
        set(KEY_UNMATCHED_RIGHT, unmatchedRightAction);
    }

    /** @return the left input object ID */
    public String getLeftInputObject() {
        return get(KEY_LEFT_INPUT);
    }

    /** @return the right input object ID */
    public String getRightInputObject() {
        return get(KEY_RIGHT_INPUT);
    }

    /** @return the left column names in the mapping */
    public String[] getLeftColumns() {
        return get(KEY_LEFT_COLUMNS);
    }

    /** @return the right column names in the mapping (empty = same name) */
    public String[] getRightColumns() {
        return get(KEY_RIGHT_COLUMNS);
    }

    /** @return the unmatched left column action (FILL_NULL or EXCLUDE) */
    public String getUnmatchedLeftAction() {
        return get(KEY_UNMATCHED_LEFT);
    }

    /** @return the unmatched right column action (FILL_NULL or EXCLUDE) */
    public String getUnmatchedRightAction() {
        return get(KEY_UNMATCHED_RIGHT);
    }
}
