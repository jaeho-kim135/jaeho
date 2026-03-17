package org.knime.bigdata.spark.dx.node.preproc.rowsplitter;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Row Splitter job.
 * Contains specs for both output DataFrames (Matches and Non-Matches).
 */
@SparkClass
public class SparkRowSplitterJobOutput extends JobOutput {

    private static final String PREVIEW = "previewData";

    /** Deserialization constructor. */
    public SparkRowSplitterJobOutput() {
    }

    /**
     * Constructor with specs for both output objects.
     *
     * @param matchOutputId the named output object ID for matching rows
     * @param matchSpec the output schema for matching rows
     * @param nonMatchOutputId the named output object ID for non-matching rows
     * @param nonMatchSpec the output schema for non-matching rows
     */
    public SparkRowSplitterJobOutput(final String matchOutputId, final IntermediateSpec matchSpec,
            final String nonMatchOutputId, final IntermediateSpec nonMatchSpec) {
        withSpec(matchOutputId, matchSpec);
        withSpec(nonMatchOutputId, nonMatchSpec);
    }

    /**
     * Sets the preview data string (formatted sample rows).
     *
     * @param data formatted result string
     */
    public void setPreviewData(final String data) {
        set(PREVIEW, data);
    }

    /**
     * @return the preview data string, or null if not set
     */
    public String getPreviewData() {
        return getOrDefault(PREVIEW, null);
    }
}
