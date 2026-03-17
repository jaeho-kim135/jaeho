package org.knime.bigdata.spark3_4.dx.jobs.preproc.rank;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankJobInput;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankNodeModel;

/**
 * Rank job run factory for Spark 3.4.
 */
public class RankJobRunFactory extends DefaultJobRunFactory<SparkRankJobInput, SparkRankJobOutput> {

    /** Constructor. */
    public RankJobRunFactory() {
        super(SparkRankNodeModel.JOB_ID, RankJob.class, SparkRankJobOutput.class);
    }
}
