package org.knime.bigdata.spark3_5.dx.jobs.preproc.numbertostring;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.numbertostring.SparkNumberToStringJobInput;
import org.knime.bigdata.spark.dx.node.preproc.numbertostring.SparkNumberToStringJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.numbertostring.SparkNumberToStringNodeModel;

/**
 * Number to String job run factory for Spark 3.5.
 */
public class NumberToStringJobRunFactory
    extends DefaultJobRunFactory<SparkNumberToStringJobInput, SparkNumberToStringJobOutput> {

    /** Constructor. */
    public NumberToStringJobRunFactory() {
        super(SparkNumberToStringNodeModel.JOB_ID, NumberToStringJob.class, SparkNumberToStringJobOutput.class);
    }
}
