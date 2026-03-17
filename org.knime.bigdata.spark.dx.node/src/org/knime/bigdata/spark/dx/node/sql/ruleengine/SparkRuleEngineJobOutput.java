package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Rule Engine job.
 */
@SparkClass
public class SparkRuleEngineJobOutput extends JobOutput {

    private static final String PREVIEW_DATA = "previewData";

    /** Deserialization constructor. */
    public SparkRuleEngineJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID (null for validation-only)
     * @param outputSpec the output schema (null for validation-only)
     */
    public SparkRuleEngineJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }

    /**
     * Sets the preview data string (formatted sample rows from validation).
     *
     * @param previewData formatted result of showString()
     */
    public void setPreviewData(final String previewData) {
        set(PREVIEW_DATA, previewData);
    }

    /**
     * @return the preview data string, or null if not set
     */
    public String getPreviewData() {
        return getOrDefault(PREVIEW_DATA, null);
    }
}
