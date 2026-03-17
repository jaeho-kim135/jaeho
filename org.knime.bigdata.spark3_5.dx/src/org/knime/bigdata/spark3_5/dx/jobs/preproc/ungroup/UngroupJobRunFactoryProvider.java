package org.knime.bigdata.spark3_5.dx.jobs.preproc.ungroup;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the ungroup job run factory for Spark 3.5.
 */
public class UngroupJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public UngroupJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new UngroupJobRunFactory());
    }
}
