package org.knime.bigdata.spark3_4.dx.jobs.sql.ruleengine;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.sql.ruleengine.SparkRuleEngineJobInput;
import org.knime.bigdata.spark.dx.node.sql.ruleengine.SparkRuleEngineJobOutput;
import org.knime.bigdata.spark.dx.node.sql.ruleengine.SparkRuleEngineNodeModel;

/**
 * Rule Engine job run factory for Spark 3.4.
 */
public class RuleEngineJobRunFactory extends DefaultJobRunFactory<SparkRuleEngineJobInput, SparkRuleEngineJobOutput> {

    /** Constructor. */
    public RuleEngineJobRunFactory() {
        super(SparkRuleEngineNodeModel.JOB_ID, RuleEngineJob.class, SparkRuleEngineJobOutput.class);
    }
}
