package org.knime.bigdata.spark.dx.node.preproc.lagcolumn;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

@SparkClass
public class SparkLagColumnJobInput extends JobInput {

    private static final String KEY_COLUMN = "column";
    private static final String KEY_ORDER_COLUMN = "orderColumn";
    private static final String KEY_DIRECTION = "direction";
    private static final String KEY_NUM_COPIES = "numCopies";
    private static final String KEY_LAG_INTERVAL = "lagInterval";
    private static final String KEY_GROUP_COLUMNS = "groupColumns";
    private static final String KEY_SKIP_INCOMPLETE = "skipIncompleteRows";
    private static final String KEY_VALIDATE_ONLY = "validateOnly";

    public SparkLagColumnJobInput() {}

    public SparkLagColumnJobInput(final String inputObject, final String outputObject,
            final String column, final String orderColumn, final String direction,
            final int numCopies, final int lagInterval, final String[] groupColumns,
            final boolean skipIncompleteRows) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(KEY_COLUMN, column);
        set(KEY_ORDER_COLUMN, orderColumn);
        set(KEY_DIRECTION, direction);
        set(KEY_NUM_COPIES, numCopies);
        set(KEY_LAG_INTERVAL, lagInterval);
        set(KEY_GROUP_COLUMNS, groupColumns);
        set(KEY_SKIP_INCOMPLETE, skipIncompleteRows);
        set(KEY_VALIDATE_ONLY, false);
    }

    public SparkLagColumnJobInput(final String inputObject,
            final String column, final String orderColumn, final String direction,
            final int numCopies, final int lagInterval, final String[] groupColumns,
            final boolean skipIncompleteRows) {
        addNamedInputObject(inputObject);
        set(KEY_COLUMN, column);
        set(KEY_ORDER_COLUMN, orderColumn);
        set(KEY_DIRECTION, direction);
        set(KEY_NUM_COPIES, numCopies);
        set(KEY_LAG_INTERVAL, lagInterval);
        set(KEY_GROUP_COLUMNS, groupColumns);
        set(KEY_SKIP_INCOMPLETE, skipIncompleteRows);
        set(KEY_VALIDATE_ONLY, true);
    }

    public String getColumn() { return get(KEY_COLUMN); }
    public String getOrderColumn() { return get(KEY_ORDER_COLUMN); }
    public String getDirection() { return get(KEY_DIRECTION); }
    public int getNumCopies() { return getInteger(KEY_NUM_COPIES); }
    public int getLagInterval() { return getInteger(KEY_LAG_INTERVAL); }
    public String[] getGroupColumns() { return getOrDefault(KEY_GROUP_COLUMNS, new String[0]); }
    public boolean isSkipIncompleteRows() { return get(KEY_SKIP_INCOMPLETE); }
    public boolean isValidateOnly() { return get(KEY_VALIDATE_ONLY); }
}
