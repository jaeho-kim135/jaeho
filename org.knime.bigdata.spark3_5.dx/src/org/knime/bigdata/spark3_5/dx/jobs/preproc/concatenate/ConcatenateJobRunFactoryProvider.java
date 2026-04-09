package org.knime.bigdata.spark3_5.dx.jobs.preproc.concatenate;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the concatenate job run factory for Spark 3.5.
 */
public class ConcatenateJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ConcatenateJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new ConcatenateJobRunFactory());
    }
}
