package org.knime.bigdata.spark3_4.dx.jobs.preproc.numbertostring;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Number to String job run factory for Spark 3.4.
 */
public class NumberToStringJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public NumberToStringJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new NumberToStringJobRunFactory());
    }
}
