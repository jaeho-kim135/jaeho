package org.knime.bigdata.spark3_4.dx.jobs.preproc.transpose;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the transpose job run factory for Spark 3.4.
 */
public class TransposeJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public TransposeJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new TransposeJobRunFactory());
    }
}
