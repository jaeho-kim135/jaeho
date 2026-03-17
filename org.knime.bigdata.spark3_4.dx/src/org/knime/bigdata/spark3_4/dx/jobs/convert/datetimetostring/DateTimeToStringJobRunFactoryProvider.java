package org.knime.bigdata.spark3_4.dx.jobs.convert.datetimetostring;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the date/time to string job run factory for Spark 3.4.
 */
public class DateTimeToStringJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public DateTimeToStringJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new DateTimeToStringJobRunFactory());
    }
}
