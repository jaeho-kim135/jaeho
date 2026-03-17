package org.knime.bigdata.spark.dx.node.preproc.ungroup;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Ungroup job.
 */
@SparkClass
public class SparkUngroupJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String EXPLODE_MODE = "explodeMode";
    private static final String DELIMITER = "delimiter";
    private static final String REMOVE_ORIGINAL = "removeOriginal";
    private static final String SKIP_NULLS = "skipNulls";
    private static final String SKIP_EMPTY = "skipEmpty";
    private static final String VALIDATE_ONLY = "validateOnly";

    /** Deserialization constructor. */
    public SparkUngroupJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param columns the target column names
     * @param explodeMode the explode mode (AUTO or STRING_SPLIT)
     * @param delimiter the delimiter for string splitting
     * @param removeOriginal whether to remove the original column
     * @param skipNulls whether to skip null rows
     * @param skipEmpty whether to skip empty collections/strings
     */
    public SparkUngroupJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String explodeMode, final String delimiter,
            final boolean removeOriginal, final boolean skipNulls, final boolean skipEmpty) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(EXPLODE_MODE, explodeMode);
        set(DELIMITER, delimiter);
        set(REMOVE_ORIGINAL, removeOriginal);
        set(SKIP_NULLS, skipNulls);
        set(SKIP_EMPTY, skipEmpty);
        set(VALIDATE_ONLY, false);
    }

    /**
     * Constructor for validation-only execution.
     *
     * @param inputObject the named input object ID
     * @param columns the target column names
     * @param explodeMode the explode mode (AUTO or STRING_SPLIT)
     * @param delimiter the delimiter for string splitting
     * @param removeOriginal whether to remove the original column
     * @param skipNulls whether to skip null rows
     * @param skipEmpty whether to skip empty collections/strings
     */
    public SparkUngroupJobInput(final String inputObject,
            final String[] columns, final String explodeMode, final String delimiter,
            final boolean removeOriginal, final boolean skipNulls, final boolean skipEmpty) {

        addNamedInputObject(inputObject);
        set(COLUMNS, columns);
        set(EXPLODE_MODE, explodeMode);
        set(DELIMITER, delimiter);
        set(REMOVE_ORIGINAL, removeOriginal);
        set(SKIP_NULLS, skipNulls);
        set(SKIP_EMPTY, skipEmpty);
        set(VALIDATE_ONLY, true);
    }

    /** @return the target column names */
    public String[] getColumns() { return get(COLUMNS); }

    /** @return the explode mode (AUTO or STRING_SPLIT) */
    public String getExplodeMode() { return getOrDefault(EXPLODE_MODE, "AUTO"); }

    /** @return the delimiter for string splitting */
    public String getDelimiter() { return getOrDefault(DELIMITER, ","); }

    /** @return whether to remove the original column */
    public boolean isRemoveOriginal() { return get(REMOVE_ORIGINAL); }

    /** @return whether to skip null rows */
    public boolean isSkipNulls() { return get(SKIP_NULLS); }

    /** @return whether to skip empty collections/strings */
    public boolean isSkipEmpty() { return get(SKIP_EMPTY); }

    /** @return whether this is a validation-only run */
    public boolean isValidateOnly() { return get(VALIDATE_ONLY); }
}
