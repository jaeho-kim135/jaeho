package org.knime.bigdata.spark3_5.dx.jobs.preproc.constantvalue;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the constant value column job run factory for Spark 3.5.
 */
public class ConstantValueColumnJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ConstantValueColumnJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new ConstantValueColumnJobRunFactory());
    }
}
