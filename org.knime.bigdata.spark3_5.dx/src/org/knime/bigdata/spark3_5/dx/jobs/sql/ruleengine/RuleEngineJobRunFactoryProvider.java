package org.knime.bigdata.spark3_5.dx.jobs.sql.ruleengine;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Rule Engine job run factory for Spark 3.5.
 */
public class RuleEngineJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public RuleEngineJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new RuleEngineJobRunFactory());
    }
}
