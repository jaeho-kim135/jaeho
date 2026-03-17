package org.knime.bigdata.spark3_4.dx.jobs.preproc.binner;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the numeric binner job run factory for Spark 3.4.
 */
public class NumericBinnerJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public NumericBinnerJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new NumericBinnerJobRunFactory());
    }
}
