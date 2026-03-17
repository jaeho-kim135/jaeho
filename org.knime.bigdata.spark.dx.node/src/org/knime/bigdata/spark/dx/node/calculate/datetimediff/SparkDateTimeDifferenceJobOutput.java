package org.knime.bigdata.spark.dx.node.calculate.datetimediff;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Date&Time Difference job.
 */
@SparkClass
public class SparkDateTimeDifferenceJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkDateTimeDifferenceJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkDateTimeDifferenceJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
