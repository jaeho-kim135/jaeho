package org.knime.bigdata.spark.dx.node.preproc.transpose;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Table Transposer job.
 */
@SparkClass
public class SparkTransposeJobInput extends JobInput {

    private static final String MAX_ROWS = "maxRows";
    private static final String ID_COLUMN = "idColumn";
    private static final String VALIDATE_ONLY = "validateOnly";

    /** Deserialization constructor. */
    public SparkTransposeJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param maxRows the maximum number of rows allowed
     * @param idColumn the ID column name (empty string if not specified)
     */
    public SparkTransposeJobInput(final String inputObject, final String outputObject,
            final int maxRows, final String idColumn) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(MAX_ROWS, maxRows);
        set(ID_COLUMN, idColumn != null ? idColumn : "");
        set(VALIDATE_ONLY, false);
    }

    /**
     * Constructor for validation-only execution.
     *
     * @param inputObject the named input object ID
     * @param maxRows the maximum number of rows allowed
     * @param idColumn the ID column name (empty string if not specified)
     */
    public SparkTransposeJobInput(final String inputObject,
            final int maxRows, final String idColumn) {
        addNamedInputObject(inputObject);
        set(MAX_ROWS, maxRows);
        set(ID_COLUMN, idColumn != null ? idColumn : "");
        set(VALIDATE_ONLY, true);
    }

    /** @return the maximum number of rows allowed */
    public int getMaxRows() {
        return get(MAX_ROWS);
    }

    /** @return the ID column name, or empty string if not specified */
    public String getIdColumn() {
        return getOrDefault(ID_COLUMN, "");
    }

    /** @return true if this is a validation-only run */
    public boolean isValidateOnly() {
        return getOrDefault(VALIDATE_ONLY, false);
    }
}
