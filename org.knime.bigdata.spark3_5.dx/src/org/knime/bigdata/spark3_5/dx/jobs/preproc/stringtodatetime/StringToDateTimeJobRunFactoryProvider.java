package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringtodatetime;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the String to Date&Time job run factory for Spark 3.5.
 */
public class StringToDateTimeJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringToDateTimeJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new StringToDateTimeJobRunFactory());
    }
}
