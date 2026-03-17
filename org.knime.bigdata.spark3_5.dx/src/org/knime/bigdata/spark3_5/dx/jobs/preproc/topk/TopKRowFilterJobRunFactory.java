package org.knime.bigdata.spark3_5.dx.jobs.preproc.topk;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.topk.SparkTopKRowFilterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.topk.SparkTopKRowFilterJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.topk.SparkTopKRowFilterNodeModel;

/**
 * Top k Row Filter job run factory for Spark 3.5.
 */
public class TopKRowFilterJobRunFactory
    extends DefaultJobRunFactory<SparkTopKRowFilterJobInput, SparkTopKRowFilterJobOutput> {

    /** Constructor. */
    public TopKRowFilterJobRunFactory() {
        super(SparkTopKRowFilterNodeModel.JOB_ID, TopKRowFilterJob.class, SparkTopKRowFilterJobOutput.class);
    }
}
