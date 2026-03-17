package org.knime.bigdata.spark.dx.node.preproc.numbertostring;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Number to String job.
 */
@SparkClass
public class SparkNumberToStringJobInput extends JobInput {

    private static final String COLUMNS = "columns";

    /** Deserialization constructor. */
    public SparkNumberToStringJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param columns the columns to convert
     */
    public SparkNumberToStringJobInput(final String inputObject, final String outputObject,
            final String[] columns) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
    }

    /** @return the columns to convert */
    public String[] getColumns() { return get(COLUMNS); }
}
