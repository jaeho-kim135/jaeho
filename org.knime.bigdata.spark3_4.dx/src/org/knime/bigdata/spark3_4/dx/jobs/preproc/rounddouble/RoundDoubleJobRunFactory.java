package org.knime.bigdata.spark3_4.dx.jobs.preproc.rounddouble;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.rounddouble.SparkRoundDoubleJobInput;
import org.knime.bigdata.spark.dx.node.preproc.rounddouble.SparkRoundDoubleJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.rounddouble.SparkRoundDoubleNodeModel;

/**
 * Round double job run factory for Spark 3.4.
 */
public class RoundDoubleJobRunFactory extends DefaultJobRunFactory<SparkRoundDoubleJobInput, SparkRoundDoubleJobOutput> {

    /** Constructor. */
    public RoundDoubleJobRunFactory() {
        super(SparkRoundDoubleNodeModel.JOB_ID, RoundDoubleJob.class, SparkRoundDoubleJobOutput.class);
    }
}
