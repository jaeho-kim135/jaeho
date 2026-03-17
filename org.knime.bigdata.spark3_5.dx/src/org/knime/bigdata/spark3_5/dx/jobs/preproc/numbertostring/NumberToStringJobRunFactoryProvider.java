package org.knime.bigdata.spark3_5.dx.jobs.preproc.numbertostring;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Number to String job run factory for Spark 3.5.
 */
public class NumberToStringJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public NumberToStringJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new NumberToStringJobRunFactory());
    }
}
