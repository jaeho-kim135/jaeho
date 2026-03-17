package org.knime.bigdata.spark3_4.dx.jobs.manipulate.datetimeshift;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.manipulate.datetimeshift.SparkDateTimeShiftJobInput;
import org.knime.bigdata.spark.dx.node.manipulate.datetimeshift.SparkDateTimeShiftJobOutput;
import org.knime.bigdata.spark.dx.node.manipulate.datetimeshift.SparkDateTimeShiftNodeModel;

/**
 * Date&amp;Time Shift job run factory for Spark 3.4.
 */
public class DateTimeShiftJobRunFactory
    extends DefaultJobRunFactory<SparkDateTimeShiftJobInput, SparkDateTimeShiftJobOutput> {

    /** Constructor. */
    public DateTimeShiftJobRunFactory() {
        super(SparkDateTimeShiftNodeModel.JOB_ID,
            DateTimeShiftJob.class,
            SparkDateTimeShiftJobOutput.class);
    }
}
