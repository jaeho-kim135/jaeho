package org.knime.bigdata.spark3_5.dx.jobs.preproc.editcolumn;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.editcolumn.SparkEditColumnJobInput;
import org.knime.bigdata.spark.dx.node.preproc.editcolumn.SparkEditColumnJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.editcolumn.SparkEditColumnNodeModel;

/**
 * Edit column job run factory for Spark 3.5.
 */
public class EditColumnJobRunFactory
    extends DefaultJobRunFactory<SparkEditColumnJobInput, SparkEditColumnJobOutput> {

    /** Constructor. */
    public EditColumnJobRunFactory() {
        super(SparkEditColumnNodeModel.JOB_ID,
            EditColumnJob.class,
            SparkEditColumnJobOutput.class);
    }
}
