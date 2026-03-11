package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringtodatetime;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the String to Date&Time job run factory for Spark 3.4.
 */
public class StringToDateTimeJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringToDateTimeJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new StringToDateTimeJobRunFactory());
    }
}
