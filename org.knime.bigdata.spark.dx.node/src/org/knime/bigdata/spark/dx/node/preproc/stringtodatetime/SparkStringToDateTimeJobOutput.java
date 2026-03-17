package org.knime.bigdata.spark.dx.node.preproc.stringtodatetime;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark String to Date&Time job.
 */
@SparkClass
public class SparkStringToDateTimeJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkStringToDateTimeJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkStringToDateTimeJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
