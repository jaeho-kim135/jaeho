package org.knime.bigdata.spark.dx.node.preproc.lagcolumn;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

@SparkClass
public class SparkLagColumnJobOutput extends JobOutput {

    private static final String PREVIEW_DATA = "previewData";

    public SparkLagColumnJobOutput() {}

    public SparkLagColumnJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }

    public void setPreviewData(final String data) { set(PREVIEW_DATA, data); }
    public String getPreviewData() { return getOrDefault(PREVIEW_DATA, null); }
}
