package org.knime.bigdata.spark.dx.node.preproc.constantvalue;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Constant Value Column job.
 * Contains the column name, value type, value, isMissing flag, and isReplace flag.
 */
@SparkClass
public class SparkConstantValueColumnJobInput extends JobInput {

    private static final String COLUMN_NAME = "columnName";
    private static final String VALUE_TYPE = "valueType";
    private static final String VALUE = "value";
    private static final String IS_MISSING = "isMissing";
    private static final String IS_REPLACE = "isReplace";

    /** Deserialization constructor. */
    public SparkConstantValueColumnJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param columnName the output column name
     * @param valueType the value type (STRING, INTEGER, LONG, DOUBLE, BOOLEAN, DATE, TIMESTAMP)
     * @param value the constant value as a string
     * @param isMissing whether the column should be filled with null
     * @param isReplace whether this replaces an existing column
     */
    public SparkConstantValueColumnJobInput(final String inputObject, final String outputObject,
            final String columnName, final String valueType, final String value,
            final boolean isMissing, final boolean isReplace) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMN_NAME, columnName);
        set(VALUE_TYPE, valueType);
        set(VALUE, value);
        set(IS_MISSING, isMissing);
        set(IS_REPLACE, isReplace);
    }

    /** @return the output column name */
    public String getColumnName() {
        return get(COLUMN_NAME);
    }

    /** @return the value type (STRING, INTEGER, LONG, DOUBLE, BOOLEAN, DATE, TIMESTAMP) */
    public String getValueType() {
        return get(VALUE_TYPE);
    }

    /** @return the constant value as a string */
    public String getValue() {
        return get(VALUE);
    }

    /** @return true if the column should be filled with null */
    public boolean isMissing() {
        return get(IS_MISSING);
    }

    /** @return true if this replaces an existing column */
    public boolean isReplace() {
        return get(IS_REPLACE);
    }
}
