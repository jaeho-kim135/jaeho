package org.knime.bigdata.spark.dx.node.preproc.rank;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Rank job.
 */
@SparkClass
public class SparkRankJobInput extends JobInput {

    private static final String RANKING_COLUMNS = "rankingColumns";
    private static final String RANKING_ORDERS = "rankingOrders";
    private static final String GROUP_COLUMNS = "groupColumns";
    private static final String RANK_MODE = "rankMode";
    private static final String OUTPUT_COL_NAME = "outputColName";
    private static final String RANK_DATA_TYPE = "rankDataType";
    private static final String MISSING_TO_END = "missingToEnd";

    /** Deserialization constructor. */
    public SparkRankJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param rankingColumns the columns to rank by
     * @param rankingOrders the sort orders for each ranking column (ASCENDING/DESCENDING)
     * @param groupColumns the columns to partition (group) by
     * @param rankMode the ranking mode (STANDARD/DENSE/ORDINAL)
     * @param outputColName the name of the output rank column
     * @param rankDataType the data type of the rank column (INTEGER/LONG)
     * @param missingToEnd whether to sort missing values to the end
     */
    public SparkRankJobInput(final String inputObject, final String outputObject,
            final String[] rankingColumns, final String[] rankingOrders,
            final String[] groupColumns, final String rankMode,
            final String outputColName, final String rankDataType,
            final boolean missingToEnd) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(RANKING_COLUMNS, rankingColumns);
        set(RANKING_ORDERS, rankingOrders);
        set(GROUP_COLUMNS, groupColumns);
        set(RANK_MODE, rankMode);
        set(OUTPUT_COL_NAME, outputColName);
        set(RANK_DATA_TYPE, rankDataType);
        set(MISSING_TO_END, missingToEnd);
    }

    /** @return the ranking columns */
    public String[] getRankingColumns() { return get(RANKING_COLUMNS); }

    /** @return the ranking orders (ASCENDING/DESCENDING per column) */
    public String[] getRankingOrders() { return getOrDefault(RANKING_ORDERS, new String[0]); }

    /** @return the group (partition) columns */
    public String[] getGroupColumns() { return getOrDefault(GROUP_COLUMNS, new String[0]); }

    /** @return the rank mode (STANDARD/DENSE/ORDINAL) */
    public String getRankMode() { return get(RANK_MODE); }

    /** @return the output rank column name */
    public String getOutputColName() { return get(OUTPUT_COL_NAME); }

    /** @return the rank data type (INTEGER/LONG) */
    public String getRankDataType() { return getOrDefault(RANK_DATA_TYPE, "LONG"); }

    /** @return whether to sort missing values to end */
    public boolean isMissingToEnd() { return getOrDefault(MISSING_TO_END, true); }
}
