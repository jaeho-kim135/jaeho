package org.knime.bigdata.spark.dx.node.preproc.constantvalue;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Constant Value Column job.
 */
@SparkClass
public class SparkConstantValueColumnJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkConstantValueColumnJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkConstantValueColumnJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
