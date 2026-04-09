package org.knime.bigdata.spark.dx.node.preproc.editcolumn;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Edit Column job.
 * Contains arrays of source column names, new names, and new types.
 */
@SparkClass
public class SparkEditColumnJobInput extends JobInput {

    private static final String KEY_SOURCE_COLUMNS = "sourceColumns";
    private static final String KEY_NEW_NAMES = "newNames";
    private static final String KEY_NEW_TYPES = "newTypes";

    /** Deserialization constructor. */
    public SparkEditColumnJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param sourceColumns the source column names (array order = output order)
     * @param newNames the new column names (empty string = keep original)
     * @param newTypes the new data types (KEEP, STRING, INTEGER, LONG, DOUBLE, FLOAT, BOOLEAN, DATE, TIMESTAMP)
     */
    public SparkEditColumnJobInput(final String inputObject, final String outputObject,
            final String[] sourceColumns, final String[] newNames, final String[] newTypes) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(KEY_SOURCE_COLUMNS, sourceColumns);
        set(KEY_NEW_NAMES, newNames);
        set(KEY_NEW_TYPES, newTypes);
    }

    /** @return the source column names */
    public String[] getSourceColumns() {
        return get(KEY_SOURCE_COLUMNS);
    }

    /** @return the new column names (empty = keep original) */
    public String[] getNewNames() {
        return get(KEY_NEW_NAMES);
    }

    /** @return the new data types (KEEP = keep original) */
    public String[] getNewTypes() {
        return get(KEY_NEW_TYPES);
    }
}
