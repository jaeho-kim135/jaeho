package org.knime.bigdata.spark.dx.node.preproc.topk;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Top k Row Filter job.
 */
@SparkClass
public class SparkTopKRowFilterJobOutput extends JobOutput {

    private static final String PREVIEW_DATA = "previewData";

    /** Deserialization constructor. */
    public SparkTopKRowFilterJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID (null for validation-only)
     * @param outputSpec the output schema (null for validation-only)
     */
    public SparkTopKRowFilterJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }

    /** Sets the preview data string. */
    public void setPreviewData(final String data) {
        set(PREVIEW_DATA, data);
    }

    /** @return the preview data string, or null if not set */
    public String getPreviewData() {
        return getOrDefault(PREVIEW_DATA, null);
    }
}
