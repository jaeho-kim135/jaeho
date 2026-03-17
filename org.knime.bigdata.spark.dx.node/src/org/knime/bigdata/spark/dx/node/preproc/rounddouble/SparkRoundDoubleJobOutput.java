package org.knime.bigdata.spark.dx.node.preproc.rounddouble;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Number Rounder job.
 */
@SparkClass
public class SparkRoundDoubleJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkRoundDoubleJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkRoundDoubleJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
