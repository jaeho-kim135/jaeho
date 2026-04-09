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
    private static final String KEY_SORT_COLUMNS = "sortColumns";
    private static final String KEY_SORT_ORDERS = "sortOrders";
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
     * @param sortColumns sort column names array
     * @param sortOrders sort order strings array (ASCENDING/DESCENDING)
     */
    public SparkTopKRowFilterJobInput(final String inputObject, final String outputObject,
            final long k, final String filterMode, final String outputOrder,
            final boolean missingsToEnd, final String[] groupColumns,
            final String[] sortColumns, final String[] sortOrders) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(KEY_K, String.valueOf(k));
        set(KEY_FILTER_MODE, filterMode);
        set(KEY_OUTPUT_ORDER, outputOrder);
        set(KEY_MISSINGS_TO_END, missingsToEnd);
        set(KEY_GROUP_COLUMNS, groupColumns);
        set(KEY_SORT_COLUMNS, sortColumns);
        set(KEY_SORT_ORDERS, sortOrders);
        set(KEY_VALIDATE_ONLY, false);
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

    /** @return the sort column names */
    public String[] getSortColumns() { return getOrDefault(KEY_SORT_COLUMNS, new String[0]); }

    /** @return the sort orders (ASCENDING/DESCENDING) */
    public String[] getSortOrders() { return getOrDefault(KEY_SORT_ORDERS, new String[0]); }

    /** @return whether this is a validation-only run */
    public boolean isValidateOnly() { return get(KEY_VALIDATE_ONLY); }
}
