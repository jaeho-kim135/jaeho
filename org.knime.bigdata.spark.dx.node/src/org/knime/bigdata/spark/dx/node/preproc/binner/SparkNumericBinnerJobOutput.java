package org.knime.bigdata.spark.dx.node.preproc.binner;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Numeric Binner job.
 */
@SparkClass
public class SparkNumericBinnerJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkNumericBinnerJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkNumericBinnerJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
