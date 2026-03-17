package org.knime.bigdata.spark.dx.node.extract.datetimefields;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Extract Date&amp;Time Fields job.
 */
@SparkClass
public class SparkExtractDateTimeFieldsJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkExtractDateTimeFieldsJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkExtractDateTimeFieldsJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
