package org.knime.bigdata.spark3_4.dx.jobs.preproc.rounddouble;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the round double job run factory for Spark 3.4.
 */
public class RoundDoubleJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public RoundDoubleJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new RoundDoubleJobRunFactory());
    }
}
