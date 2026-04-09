package org.knime.bigdata.spark3_4.dx.jobs.preproc.concatenate;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the concatenate job run factory for Spark 3.4.
 */
public class ConcatenateJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ConcatenateJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new ConcatenateJobRunFactory());
    }
}
