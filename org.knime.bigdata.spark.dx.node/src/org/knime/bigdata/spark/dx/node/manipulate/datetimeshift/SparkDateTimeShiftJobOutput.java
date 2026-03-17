package org.knime.bigdata.spark.dx.node.manipulate.datetimeshift;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Date&amp;Time Shift job.
 */
@SparkClass
public class SparkDateTimeShiftJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkDateTimeShiftJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkDateTimeShiftJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }
}
