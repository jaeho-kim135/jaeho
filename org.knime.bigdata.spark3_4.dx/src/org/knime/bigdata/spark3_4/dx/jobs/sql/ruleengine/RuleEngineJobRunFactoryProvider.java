package org.knime.bigdata.spark3_4.dx.jobs.sql.ruleengine;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Rule Engine job run factory for Spark 3.4.
 */
public class RuleEngineJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public RuleEngineJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new RuleEngineJobRunFactory());
    }
}
