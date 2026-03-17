package org.knime.bigdata.spark.dx.node.preproc.duplicates;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Duplicate Row Filter job.
 */
@SparkClass
public class SparkDuplicateRowFilterJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkDuplicateRowFilterJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkDuplicateRowFilterJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
