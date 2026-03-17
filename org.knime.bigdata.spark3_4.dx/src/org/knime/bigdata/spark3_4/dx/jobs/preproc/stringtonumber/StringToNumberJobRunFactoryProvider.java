package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringtonumber;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the String to Number job run factory for Spark 3.4.
 */
public class StringToNumberJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringToNumberJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new StringToNumberJobRunFactory());
    }
}
