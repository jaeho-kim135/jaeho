package org.knime.bigdata.spark3_4.dx.jobs.preproc.rank;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the rank job run factory for Spark 3.4.
 */
public class RankJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public RankJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new RankJobRunFactory());
    }
}
