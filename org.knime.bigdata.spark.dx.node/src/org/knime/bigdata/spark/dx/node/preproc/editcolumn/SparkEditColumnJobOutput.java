package org.knime.bigdata.spark.dx.node.preproc.editcolumn;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Edit Column job.
 */
@SparkClass
public class SparkEditColumnJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkEditColumnJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkEditColumnJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        withSpec(outputObject, outputSpec);
    }
}
