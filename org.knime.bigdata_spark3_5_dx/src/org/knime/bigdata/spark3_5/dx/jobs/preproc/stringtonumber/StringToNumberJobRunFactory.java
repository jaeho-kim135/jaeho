package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringtonumber;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringtonumber.SparkStringToNumberJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringtonumber.SparkStringToNumberJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.stringtonumber.SparkStringToNumberNodeModel;

/**
 * String to Number job run factory for Spark 3.5.
 */
public class StringToNumberJobRunFactory
    extends DefaultJobRunFactory<SparkStringToNumberJobInput, SparkStringToNumberJobOutput> {

    /** Constructor. */
    public StringToNumberJobRunFactory() {
        super(SparkStringToNumberNodeModel.JOB_ID, StringToNumberJob.class, SparkStringToNumberJobOutput.class);
    }
}
