package org.knime.bigdata.spark3_4.dx.jobs.preproc.ungroup;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the ungroup job run factory for Spark 3.4.
 */
public class UngroupJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public UngroupJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new UngroupJobRunFactory());
    }
}
