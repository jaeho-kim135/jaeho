package org.knime.bigdata.spark3_4.dx.jobs.extract.datetimefields;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Extract Date&amp;Time Fields job run factory for Spark 3.4.
 */
public class ExtractDateTimeFieldsJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ExtractDateTimeFieldsJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new ExtractDateTimeFieldsJobRunFactory());
    }
}
