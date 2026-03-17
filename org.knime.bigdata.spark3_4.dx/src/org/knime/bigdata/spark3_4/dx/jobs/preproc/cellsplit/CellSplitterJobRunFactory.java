package org.knime.bigdata.spark3_4.dx.jobs.preproc.cellsplit;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.cellsplit.SparkCellSplitterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.cellsplit.SparkCellSplitterJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.cellsplit.SparkCellSplitterNodeModel;

/**
 * Cell Splitter job run factory for Spark 3.4.
 */
public class CellSplitterJobRunFactory
    extends DefaultJobRunFactory<SparkCellSplitterJobInput, SparkCellSplitterJobOutput> {

    /** Constructor. */
    public CellSplitterJobRunFactory() {
        super(SparkCellSplitterNodeModel.JOB_ID, CellSplitterJob.class, SparkCellSplitterJobOutput.class);
    }
}
