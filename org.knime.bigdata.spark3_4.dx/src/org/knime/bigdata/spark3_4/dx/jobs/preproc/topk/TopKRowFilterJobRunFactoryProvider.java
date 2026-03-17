package org.knime.bigdata.spark3_4.dx.jobs.preproc.topk;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Top k Row Filter job run factory for Spark 3.4.
 */
public class TopKRowFilterJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public TopKRowFilterJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new TopKRowFilterJobRunFactory());
    }
}
