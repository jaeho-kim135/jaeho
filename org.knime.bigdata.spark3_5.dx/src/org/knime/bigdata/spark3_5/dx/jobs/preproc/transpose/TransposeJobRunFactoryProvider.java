package org.knime.bigdata.spark3_5.dx.jobs.preproc.transpose;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the transpose job run factory for Spark 3.5.
 */
public class TransposeJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public TransposeJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new TransposeJobRunFactory());
    }
}
