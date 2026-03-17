package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringmanip;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the string manipulation job run factory for Spark 3.5.
 */
public class StringManipJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringManipJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new StringManipJobRunFactory());
    }
}
