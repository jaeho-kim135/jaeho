package org.knime.bigdata.spark3_5.dx.jobs.extract.datetimefields;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Extract Date&amp;Time Fields job run factory for Spark 3.5.
 */
public class ExtractDateTimeFieldsJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ExtractDateTimeFieldsJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new ExtractDateTimeFieldsJobRunFactory());
    }
}
