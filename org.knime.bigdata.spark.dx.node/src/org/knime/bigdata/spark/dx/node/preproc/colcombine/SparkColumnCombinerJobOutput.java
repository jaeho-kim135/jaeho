package org.knime.bigdata.spark.dx.node.preproc.colcombine;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Column Combiner job.
 */
@SparkClass
public class SparkColumnCombinerJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkColumnCombinerJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkColumnCombinerJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
