package org.knime.bigdata.spark3_5.dx.jobs.preproc.binner;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the numeric binner job run factory for Spark 3.5.
 */
public class NumericBinnerJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public NumericBinnerJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new NumericBinnerJobRunFactory());
    }
}
