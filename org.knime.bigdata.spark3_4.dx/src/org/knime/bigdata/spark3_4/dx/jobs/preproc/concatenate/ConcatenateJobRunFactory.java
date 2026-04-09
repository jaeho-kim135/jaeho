package org.knime.bigdata.spark3_4.dx.jobs.preproc.concatenate;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.concatenate.SparkConcatenateJobInput;
import org.knime.bigdata.spark.dx.node.preproc.concatenate.SparkConcatenateJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.concatenate.SparkConcatenateNodeModel;

/**
 * Concatenate job run factory for Spark 3.4.
 */
public class ConcatenateJobRunFactory
    extends DefaultJobRunFactory<SparkConcatenateJobInput, SparkConcatenateJobOutput> {

    /** Constructor. */
    public ConcatenateJobRunFactory() {
        super(SparkConcatenateNodeModel.JOB_ID,
            ConcatenateJob.class,
            SparkConcatenateJobOutput.class);
    }
}
