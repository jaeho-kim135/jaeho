package org.knime.bigdata.spark3_4.dx.jobs.calculate.datetimediff;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Date&Time Difference job run factory for Spark 3.4.
 */
public class DateTimeDifferenceJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public DateTimeDifferenceJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new DateTimeDifferenceJobRunFactory());
    }
}
