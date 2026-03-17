package org.knime.bigdata.spark3_5.dx.jobs.preproc.constantvalue;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.constantvalue.SparkConstantValueColumnJobInput;
import org.knime.bigdata.spark.dx.node.preproc.constantvalue.SparkConstantValueColumnJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.constantvalue.SparkConstantValueColumnNodeModel;

/**
 * Constant value column job run factory for Spark 3.5.
 */
public class ConstantValueColumnJobRunFactory
    extends DefaultJobRunFactory<SparkConstantValueColumnJobInput, SparkConstantValueColumnJobOutput> {

    /** Constructor. */
    public ConstantValueColumnJobRunFactory() {
        super(SparkConstantValueColumnNodeModel.JOB_ID,
            ConstantValueColumnJob.class,
            SparkConstantValueColumnJobOutput.class);
    }
}
