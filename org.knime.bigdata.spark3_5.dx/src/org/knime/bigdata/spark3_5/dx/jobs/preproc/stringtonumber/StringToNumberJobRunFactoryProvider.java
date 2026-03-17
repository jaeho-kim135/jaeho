package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringtonumber;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the String to Number job run factory for Spark 3.5.
 */
public class StringToNumberJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringToNumberJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new StringToNumberJobRunFactory());
    }
}
