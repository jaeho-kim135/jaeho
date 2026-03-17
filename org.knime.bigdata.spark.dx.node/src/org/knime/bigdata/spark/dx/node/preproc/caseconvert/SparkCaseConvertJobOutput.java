package org.knime.bigdata.spark.dx.node.preproc.caseconvert;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Case Converter job.
 * Optionally carries preview data for the Evaluate button.
 */
@SparkClass
public class SparkCaseConvertJobOutput extends JobOutput {

    private static final String PREVIEW_DATA = "previewData";

    /** Deserialization constructor. */
    public SparkCaseConvertJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID (may be null for validate-only)
     * @param outputSpec the output schema (may be null for validate-only)
     */
    public SparkCaseConvertJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }

    /** Sets the preview data string (for Evaluate button). */
    public void setPreviewData(final String data) {
        set(PREVIEW_DATA, data);
    }

    /** @return the preview data string, or null if not set */
    public String getPreviewData() {
        return getOrDefault(PREVIEW_DATA, null);
    }
}
