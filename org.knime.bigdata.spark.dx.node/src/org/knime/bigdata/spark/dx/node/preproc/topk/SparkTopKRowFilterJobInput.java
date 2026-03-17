package org.knime.bigdata.spark.dx.node.preproc.topk;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Top k Row Filter job.
 */
@SparkClass
public class SparkTopKRowFilterJobInput extends JobInput {

    private static final String KEY_K = "k";
    private static final String KEY_FILTER_MODE = "filterMode";
    private static final String KEY_OUTPUT_ORDER = "outputOrder";
    private static final String KEY_MISSINGS_TO_END = "missingsToEnd";
    private static final String KEY_GROUP_COLUMNS = "groupColumns";
    private static final String KEY_SORT_COLUMN1 = "sortColumn1";
    private static final String KEY_SORT_ORDER1 = "sortOrder1";
    private static final String KEY_SORT_COLUMN2 = "sortColumn2";
    private static final String KEY_SORT_ORDER2 = "sortOrder2";
    private static final String KEY_USE_SECOND_SORT = "useSecondSort";
    private static final String KEY_VALIDATE_ONLY = "validateOnly";

    /** Deserialization constructor. */
    public SparkTopKRowFilterJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param k number of rows to select
     * @param filterMode ROWS or UNIQUE_VALUES
     * @param outputOrder SORTED or ARBITRARY
     * @param missingsToEnd whether to put null values at end of sort
     * @param groupColumns group-by columns for per-group top-k
     * @param sortColumn1 primary sort column
     * @param sortOrder1 primary sort order (ASCENDING/DESCENDING)
     * @param sortColumn2 secondary sort column (may be empty)
     * @param sortOrder2 secondary sort order (ASCENDING/DESCENDING)
     * @param useSecondSort whether to use the second sort criterion
     */
    public SparkTopKRowFilterJobInput(final String inputObject, final String outputObject,
            final long k, final String filterMode, final String outputOrder,
            final boolean missingsToEnd, final String[] groupColumns,
            final String sortColumn1, final String sortOrder1,
            final String sortColumn2, final String sortOrder2,
            final boolean useSecondSort) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(KEY_K, String.valueOf(k));
        set(KEY_FILTER_MODE, filterMode);
        set(KEY_OUTPUT_ORDER, outputOrder);
        set(KEY_MISSINGS_TO_END, missingsToEnd);
        set(KEY_GROUP_COLUMNS, groupColumns);
        set(KEY_SORT_COLUMN1, sortColumn1);
        set(KEY_SORT_ORDER1, sortOrder1);
        set(KEY_SORT_COLUMN2, sortColumn2 != null ? sortColumn2 : "");
        set(KEY_SORT_ORDER2, sortOrder2 != null ? sortOrder2 : "DESCENDING");
        set(KEY_USE_SECOND_SORT, useSecondSort);
        set(KEY_VALIDATE_ONLY, false);
    }

    /**
     * Constructor for validation-only execution (no output object).
     *
     * @param inputObject the named input object ID
     * @param k number of rows to select
     * @param filterMode ROWS or UNIQUE_VALUES
     * @param outputOrder SORTED or ARBITRARY
     * @param missingsToEnd whether to put null values at end of sort
     * @param groupColumns group-by columns for per-group top-k
     * @param sortColumn1 primary sort column
     * @param sortOrder1 primary sort order (ASCENDING/DESCENDING)
     * @param sortColumn2 secondary sort column (may be empty)
     * @param sortOrder2 secondary sort order (ASCENDING/DESCENDING)
     * @param useSecondSort whether to use the second sort criterion
     */
    public SparkTopKRowFilterJobInput(final String inputObject,
            final long k, final String filterMode, final String outputOrder,
            final boolean missingsToEnd, final String[] groupColumns,
            final String sortColumn1, final String sortOrder1,
            final String sortColumn2, final String sortOrder2,
            final boolean useSecondSort) {

        addNamedInputObject(inputObject);
        set(KEY_K, String.valueOf(k));
        set(KEY_FILTER_MODE, filterMode);
        set(KEY_OUTPUT_ORDER, outputOrder);
        set(KEY_MISSINGS_TO_END, missingsToEnd);
        set(KEY_GROUP_COLUMNS, groupColumns);
        set(KEY_SORT_COLUMN1, sortColumn1);
        set(KEY_SORT_ORDER1, sortOrder1);
        set(KEY_SORT_COLUMN2, sortColumn2 != null ? sortColumn2 : "");
        set(KEY_SORT_ORDER2, sortOrder2 != null ? sortOrder2 : "DESCENDING");
        set(KEY_USE_SECOND_SORT, useSecondSort);
        set(KEY_VALIDATE_ONLY, true);
    }

    /** @return the number of rows to select */
    public long getK() {
        String v = get(KEY_K);
        return Long.parseLong(v);
    }

    /** @return the filter mode (ROWS or UNIQUE_VALUES) */
    public String getFilterMode() { return get(KEY_FILTER_MODE); }

    /** @return the output order (SORTED or ARBITRARY) */
    public String getOutputOrder() { return get(KEY_OUTPUT_ORDER); }

    /** @return whether to put null values at end of sort */
    public boolean isMissingsToEnd() { return get(KEY_MISSINGS_TO_END); }

    /** @return the group-by columns */
    public String[] getGroupColumns() { return getOrDefault(KEY_GROUP_COLUMNS, new String[0]); }

    /** @return the primary sort column */
    public String getSortColumn1() { return get(KEY_SORT_COLUMN1); }

    /** @return the primary sort order */
    public String getSortOrder1() { return get(KEY_SORT_ORDER1); }

    /** @return the secondary sort column */
    public String getSortColumn2() { return getOrDefault(KEY_SORT_COLUMN2, ""); }

    /** @return the secondary sort order */
    public String getSortOrder2() { return getOrDefault(KEY_SORT_ORDER2, "DESCENDING"); }

    /** @return whether to use the second sort criterion */
    public boolean useSecondSort() { return get(KEY_USE_SECOND_SORT); }

    /** @return whether this is a validation-only run */
    public boolean isValidateOnly() { return get(KEY_VALIDATE_ONLY); }
}
