package org.knime.bigdata.spark.dx.node.preproc.cellsplit;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Cell Splitter job.
 */
@SparkClass
public class SparkCellSplitterJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkCellSplitterJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkCellSplitterJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
