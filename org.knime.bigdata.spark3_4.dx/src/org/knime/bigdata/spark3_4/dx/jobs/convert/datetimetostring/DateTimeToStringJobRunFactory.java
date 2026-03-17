package org.knime.bigdata.spark3_4.dx.jobs.convert.datetimetostring;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.convert.datetimetostring.SparkDateTimeToStringJobInput;
import org.knime.bigdata.spark.dx.node.convert.datetimetostring.SparkDateTimeToStringJobOutput;
import org.knime.bigdata.spark.dx.node.convert.datetimetostring.SparkDateTimeToStringNodeModel;

/**
 * Date/time to string job run factory for Spark 3.4.
 */
public class DateTimeToStringJobRunFactory
    extends DefaultJobRunFactory<SparkDateTimeToStringJobInput, SparkDateTimeToStringJobOutput> {

    /** Constructor. */
    public DateTimeToStringJobRunFactory() {
        super(SparkDateTimeToStringNodeModel.JOB_ID,
            DateTimeToStringJob.class,
            SparkDateTimeToStringJobOutput.class);
    }
}
