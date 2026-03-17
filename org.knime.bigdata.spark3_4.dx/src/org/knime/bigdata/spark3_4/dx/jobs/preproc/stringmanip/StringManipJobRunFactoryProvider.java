package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringmanip;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the string manipulation job run factory for Spark 3.4.
 */
public class StringManipJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringManipJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new StringManipJobRunFactory());
    }
}
