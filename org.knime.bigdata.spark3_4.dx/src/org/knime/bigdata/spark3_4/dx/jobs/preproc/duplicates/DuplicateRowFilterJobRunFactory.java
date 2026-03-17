package org.knime.bigdata.spark3_4.dx.jobs.preproc.duplicates;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterNodeModel;

/**
 * Duplicate Row Filter job run factory for Spark 3.4.
 */
public class DuplicateRowFilterJobRunFactory
    extends DefaultJobRunFactory<SparkDuplicateRowFilterJobInput, SparkDuplicateRowFilterJobOutput> {

    /** Constructor. */
    public DuplicateRowFilterJobRunFactory() {
        super(SparkDuplicateRowFilterNodeModel.JOB_ID,
            DuplicateRowFilterJob.class, SparkDuplicateRowFilterJobOutput.class);
    }
}
