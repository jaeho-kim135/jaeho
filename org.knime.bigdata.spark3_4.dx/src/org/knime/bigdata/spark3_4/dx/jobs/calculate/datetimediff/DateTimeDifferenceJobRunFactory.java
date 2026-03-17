package org.knime.bigdata.spark3_4.dx.jobs.calculate.datetimediff;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.calculate.datetimediff.SparkDateTimeDifferenceJobInput;
import org.knime.bigdata.spark.dx.node.calculate.datetimediff.SparkDateTimeDifferenceJobOutput;
import org.knime.bigdata.spark.dx.node.calculate.datetimediff.SparkDateTimeDifferenceNodeModel;

/**
 * Date&Time Difference job run factory for Spark 3.4.
 */
public class DateTimeDifferenceJobRunFactory
    extends DefaultJobRunFactory<SparkDateTimeDifferenceJobInput, SparkDateTimeDifferenceJobOutput> {

    /** Constructor. */
    public DateTimeDifferenceJobRunFactory() {
        super(SparkDateTimeDifferenceNodeModel.JOB_ID, DateTimeDifferenceJob.class,
            SparkDateTimeDifferenceJobOutput.class);
    }
}
