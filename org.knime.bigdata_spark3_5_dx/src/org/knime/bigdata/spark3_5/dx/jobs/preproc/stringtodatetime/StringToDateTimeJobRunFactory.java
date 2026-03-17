package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringtodatetime;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringtodatetime.SparkStringToDateTimeJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringtodatetime.SparkStringToDateTimeJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.stringtodatetime.SparkStringToDateTimeNodeModel;

/**
 * String to Date&Time job run factory for Spark 3.5.
 */
public class StringToDateTimeJobRunFactory
    extends DefaultJobRunFactory<SparkStringToDateTimeJobInput, SparkStringToDateTimeJobOutput> {

    /** Constructor. */
    public StringToDateTimeJobRunFactory() {
        super(SparkStringToDateTimeNodeModel.JOB_ID, StringToDateTimeJob.class, SparkStringToDateTimeJobOutput.class);
    }
}
