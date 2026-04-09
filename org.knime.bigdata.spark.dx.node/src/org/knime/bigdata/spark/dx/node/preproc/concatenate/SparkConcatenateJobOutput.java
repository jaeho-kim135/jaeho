package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Concatenate job.
 */
@SparkClass
public class SparkConcatenateJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkConcatenateJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkConcatenateJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        withSpec(outputObject, outputSpec);
    }
}
