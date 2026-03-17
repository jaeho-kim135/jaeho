package org.knime.bigdata.spark3_5.dx.jobs.calculate.datetimediff;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Date&Time Difference job run factory for Spark 3.5.
 */
public class DateTimeDifferenceJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public DateTimeDifferenceJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new DateTimeDifferenceJobRunFactory());
    }
}
