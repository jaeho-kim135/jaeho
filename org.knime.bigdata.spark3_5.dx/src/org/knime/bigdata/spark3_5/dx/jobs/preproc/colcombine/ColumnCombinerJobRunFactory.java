package org.knime.bigdata.spark3_5.dx.jobs.preproc.colcombine;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.colcombine.SparkColumnCombinerJobInput;
import org.knime.bigdata.spark.dx.node.preproc.colcombine.SparkColumnCombinerJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.colcombine.SparkColumnCombinerNodeModel;

/**
 * Column Combiner job run factory for Spark 3.5.
 */
public class ColumnCombinerJobRunFactory
    extends DefaultJobRunFactory<SparkColumnCombinerJobInput, SparkColumnCombinerJobOutput> {

    /** Constructor. */
    public ColumnCombinerJobRunFactory() {
        super(SparkColumnCombinerNodeModel.JOB_ID, ColumnCombinerJob.class, SparkColumnCombinerJobOutput.class);
    }
}
