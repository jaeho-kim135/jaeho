package org.knime.bigdata.spark.dx.node.preproc.ungroup;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Ungroup job.
 */
@SparkClass
public class SparkUngroupJobOutput extends JobOutput {

    private static final String PREVIEW_DATA = "previewData";

    /** Deserialization constructor. */
    public SparkUngroupJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID (null for validation-only)
     * @param outputSpec the output schema (null for validation-only)
     */
    public SparkUngroupJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }

    /** Sets the preview data string. */
    public void setPreviewData(final String previewData) {
        set(PREVIEW_DATA, previewData);
    }

    /** @return the preview data string, or null if not set */
    public String getPreviewData() {
        return getOrDefault(PREVIEW_DATA, null);
    }
}
