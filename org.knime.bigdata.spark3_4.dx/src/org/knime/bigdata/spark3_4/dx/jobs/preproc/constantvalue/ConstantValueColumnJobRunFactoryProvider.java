package org.knime.bigdata.spark3_4.dx.jobs.preproc.constantvalue;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the constant value column job run factory for Spark 3.4.
 */
public class ConstantValueColumnJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ConstantValueColumnJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new ConstantValueColumnJobRunFactory());
    }
}
