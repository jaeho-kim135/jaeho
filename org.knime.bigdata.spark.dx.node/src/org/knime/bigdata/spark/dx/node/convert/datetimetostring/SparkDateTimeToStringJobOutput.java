package org.knime.bigdata.spark.dx.node.convert.datetimetostring;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Date&amp;Time to String job.
 */
@SparkClass
public class SparkDateTimeToStringJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkDateTimeToStringJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkDateTimeToStringJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        withSpec(outputObject, outputSpec);
    }
}
