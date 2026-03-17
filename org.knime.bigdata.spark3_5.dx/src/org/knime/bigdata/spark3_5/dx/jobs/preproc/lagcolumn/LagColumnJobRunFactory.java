package org.knime.bigdata.spark3_5.dx.jobs.preproc.lagcolumn;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.lagcolumn.SparkLagColumnJobInput;
import org.knime.bigdata.spark.dx.node.preproc.lagcolumn.SparkLagColumnJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.lagcolumn.SparkLagColumnNodeModel;

public class LagColumnJobRunFactory extends DefaultJobRunFactory<SparkLagColumnJobInput, SparkLagColumnJobOutput> {
    public LagColumnJobRunFactory() {
        super(SparkLagColumnNodeModel.JOB_ID, LagColumnJob.class, SparkLagColumnJobOutput.class);
    }
}
