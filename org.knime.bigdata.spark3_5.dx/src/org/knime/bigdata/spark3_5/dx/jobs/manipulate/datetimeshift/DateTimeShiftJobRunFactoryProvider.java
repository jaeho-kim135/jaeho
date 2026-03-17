package org.knime.bigdata.spark3_5.dx.jobs.manipulate.datetimeshift;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Date&amp;Time Shift job run factory for Spark 3.5.
 */
public class DateTimeShiftJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public DateTimeShiftJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new DateTimeShiftJobRunFactory());
    }
}
