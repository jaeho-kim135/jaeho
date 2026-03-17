package org.knime.bigdata.spark3_4.dx.jobs.preproc.transpose;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.transpose.SparkTransposeJobInput;
import org.knime.bigdata.spark.dx.node.preproc.transpose.SparkTransposeJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.transpose.SparkTransposeNodeModel;

/**
 * Transpose job run factory for Spark 3.4.
 */
public class TransposeJobRunFactory extends DefaultJobRunFactory<SparkTransposeJobInput, SparkTransposeJobOutput> {

    /** Constructor. */
    public TransposeJobRunFactory() {
        super(SparkTransposeNodeModel.JOB_ID, TransposeJob.class, SparkTransposeJobOutput.class);
    }
}
