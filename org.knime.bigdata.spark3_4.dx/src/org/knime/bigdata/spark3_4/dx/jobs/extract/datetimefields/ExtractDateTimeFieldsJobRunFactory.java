package org.knime.bigdata.spark3_4.dx.jobs.extract.datetimefields;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.extract.datetimefields.SparkExtractDateTimeFieldsJobInput;
import org.knime.bigdata.spark.dx.node.extract.datetimefields.SparkExtractDateTimeFieldsJobOutput;
import org.knime.bigdata.spark.dx.node.extract.datetimefields.SparkExtractDateTimeFieldsNodeModel;

/**
 * Extract Date&amp;Time Fields job run factory for Spark 3.4.
 */
public class ExtractDateTimeFieldsJobRunFactory
    extends DefaultJobRunFactory<SparkExtractDateTimeFieldsJobInput, SparkExtractDateTimeFieldsJobOutput> {

    /** Constructor. */
    public ExtractDateTimeFieldsJobRunFactory() {
        super(SparkExtractDateTimeFieldsNodeModel.JOB_ID,
            ExtractDateTimeFieldsJob.class,
            SparkExtractDateTimeFieldsJobOutput.class);
    }
}
