package org.knime.bigdata.spark.dx.node.preproc.duplicates;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Duplicate Row Filter job.
 */
@SparkClass
public class SparkDuplicateRowFilterJobInput extends JobInput {

    private static final String DUPLICATE_COLUMNS = "duplicateColumns";
    private static final String DUPLICATE_HANDLING = "duplicateHandling";
    private static final String ROW_SELECTION = "rowSelection";
    private static final String ORDER_COLUMN = "orderColumn";
    private static final String ORDER_DIRECTION = "orderDirection";
    private static final String ADD_STATUS_COLUMN = "addStatusColumn";
    private static final String STATUS_COLUMN_NAME = "statusColumnName";

    /** Deserialization constructor. */
    public SparkDuplicateRowFilterJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param duplicateColumns columns used to detect duplicates
     * @param duplicateHandling REMOVE or KEEP
     * @param rowSelection FIRST, LAST, MINIMUM, MAXIMUM, or REMOVE_ALL
     * @param orderColumn column used for ordering within duplicate groups
     * @param orderDirection ASC or DESC
     * @param addStatusColumn whether to add a status column (KEEP mode)
     * @param statusColumnName name of the status column
     */
    public SparkDuplicateRowFilterJobInput(final String inputObject, final String outputObject,
            final String[] duplicateColumns, final String duplicateHandling,
            final String rowSelection, final String orderColumn, final String orderDirection,
            final boolean addStatusColumn, final String statusColumnName) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(DUPLICATE_COLUMNS, duplicateColumns);
        set(DUPLICATE_HANDLING, duplicateHandling);
        set(ROW_SELECTION, rowSelection);
        set(ORDER_COLUMN, orderColumn);
        set(ORDER_DIRECTION, orderDirection);
        set(ADD_STATUS_COLUMN, addStatusColumn);
        set(STATUS_COLUMN_NAME, statusColumnName);
    }

    /** @return columns used to detect duplicates */
    public String[] getDuplicateColumns() {
        return get(DUPLICATE_COLUMNS);
    }

    /** @return the duplicate handling mode (REMOVE/KEEP) */
    public String getDuplicateHandling() {
        return get(DUPLICATE_HANDLING);
    }

    /** @return the row selection mode (FIRST/LAST/MINIMUM/MAXIMUM/REMOVE_ALL) */
    public String getRowSelection() {
        return get(ROW_SELECTION);
    }

    /** @return the order column name */
    public String getOrderColumn() {
        return getOrDefault(ORDER_COLUMN, "");
    }

    /** @return the order direction (ASC/DESC) */
    public String getOrderDirection() {
        return getOrDefault(ORDER_DIRECTION, "ASC");
    }

    /** @return true if order direction is ascending */
    public boolean isAscending() {
        return "ASC".equals(getOrderDirection());
    }

    /** @return whether to add a status column */
    public boolean isAddStatusColumn() {
        return getOrDefault(ADD_STATUS_COLUMN, false);
    }

    /** @return the status column name */
    public String getStatusColumnName() {
        return getOrDefault(STATUS_COLUMN_NAME, "Duplicate Status");
    }
}
