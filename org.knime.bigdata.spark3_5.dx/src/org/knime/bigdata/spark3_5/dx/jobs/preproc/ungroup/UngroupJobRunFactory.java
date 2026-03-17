package org.knime.bigdata.spark3_5.dx.jobs.preproc.ungroup;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.ungroup.SparkUngroupJobInput;
import org.knime.bigdata.spark.dx.node.preproc.ungroup.SparkUngroupJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.ungroup.SparkUngroupNodeModel;

/**
 * Ungroup job run factory for Spark 3.5.
 */
public class UngroupJobRunFactory extends DefaultJobRunFactory<SparkUngroupJobInput, SparkUngroupJobOutput> {

    /** Constructor. */
    public UngroupJobRunFactory() {
        super(SparkUngroupNodeModel.JOB_ID, UngroupJob.class, SparkUngroupJobOutput.class);
    }
}
