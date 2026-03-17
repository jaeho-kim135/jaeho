package org.knime.bigdata.spark3_4.dx.jobs.preproc.binner;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.binner.SparkNumericBinnerJobInput;
import org.knime.bigdata.spark.dx.node.preproc.binner.SparkNumericBinnerJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.binner.SparkNumericBinnerNodeModel;

/**
 * Numeric Binner job run factory for Spark 3.4.
 */
public class NumericBinnerJobRunFactory
    extends DefaultJobRunFactory<SparkNumericBinnerJobInput, SparkNumericBinnerJobOutput> {

    /** Constructor. */
    public NumericBinnerJobRunFactory() {
        super(SparkNumericBinnerNodeModel.JOB_ID, NumericBinnerJob.class, SparkNumericBinnerJobOutput.class);
    }
}
