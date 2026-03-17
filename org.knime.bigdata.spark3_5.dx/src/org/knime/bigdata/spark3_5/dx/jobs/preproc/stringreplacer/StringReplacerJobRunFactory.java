package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringreplacer;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringreplacer.SparkStringReplacerJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringreplacer.SparkStringReplacerJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.stringreplacer.SparkStringReplacerNodeModel;

/**
 * String Replacer job run factory for Spark 3.5.
 */
public class StringReplacerJobRunFactory
    extends DefaultJobRunFactory<SparkStringReplacerJobInput, SparkStringReplacerJobOutput> {

    /** Constructor. */
    public StringReplacerJobRunFactory() {
        super(SparkStringReplacerNodeModel.JOB_ID, StringReplacerJob.class, SparkStringReplacerJobOutput.class);
    }
}
