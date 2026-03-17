package org.knime.bigdata.spark.dx.node.preproc.stringtonumber;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark String to Number job.
 */
@SparkClass
public class SparkStringToNumberJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkStringToNumberJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkStringToNumberJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
