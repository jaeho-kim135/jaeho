package org.knime.bigdata.spark3_5.dx.jobs.preproc.rowsplitter;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.rowsplitter.SparkRowSplitterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.rowsplitter.SparkRowSplitterJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.rowsplitter.SparkRowSplitterNodeModel;

/**
 * Row Splitter job run factory for Spark 3.5.
 */
public class RowSplitterJobRunFactory extends DefaultJobRunFactory<SparkRowSplitterJobInput, SparkRowSplitterJobOutput> {

    /** Constructor. */
    public RowSplitterJobRunFactory() {
        super(SparkRowSplitterNodeModel.JOB_ID, RowSplitterJob.class, SparkRowSplitterJobOutput.class);
    }
}
