package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringmanip;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringmanip.SparkStringManipJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringmanip.SparkStringManipJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.stringmanip.SparkStringManipNodeModel;

/**
 * String manipulation job run factory for Spark 3.4.
 */
public class StringManipJobRunFactory extends DefaultJobRunFactory<SparkStringManipJobInput, SparkStringManipJobOutput> {

    /** Constructor. */
    public StringManipJobRunFactory() {
        super(SparkStringManipNodeModel.JOB_ID, StringManipJob.class, SparkStringManipJobOutput.class);
    }
}
