package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Rule Engine node. Converts IF-THEN rules
 * to Spark SQL CASE WHEN expressions for row-level classification.
 */
@SuppressWarnings("restriction")
public final class SparkRuleEngineNodeFactory
    extends WebUINodeFactory<SparkRuleEngineNodeModel>
    implements SparkNodeFactory<SparkRuleEngineNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Rule Engine (Hyim)")
        .icon("icon.png")
        .shortDescription("Applies IF-THEN rules to a Spark DataFrame using CASE WHEN expressions.")
        .fullDescription("""
            <p>Converts user-defined IF-THEN rules into Spark SQL CASE WHEN expressions
            and applies them to each row of the input Spark DataFrame.</p>
            <p>Rules are specified one per line in the format: <tt>condition =&gt; outcome</tt></p>
            <p>Supported syntax:</p>
            <ul>
              <li>Column references: <tt>$column_name$</tt></li>
              <li>Comparison operators: <tt>&gt;</tt>, <tt>&lt;</tt>, <tt>&gt;=</tt>, <tt>&lt;=</tt>, <tt>=</tt>, <tt>!=</tt></li>
              <li>Logical operators: <tt>AND</tt>, <tt>OR</tt>, <tt>NOT</tt>, <tt>XOR</tt></li>
              <li>Missing check: <tt>$col$ IS MISSING</tt>, <tt>$col$ IS NOT MISSING</tt></li>
              <li>Pattern matching: <tt>$col$ LIKE "pattern"</tt>, <tt>$col$ MATCHES "regex"</tt></li>
              <li>IN operator: <tt>$col$ IN ("a", "b", "c")</tt></li>
              <li>Outcome values: string <tt>"text"</tt>, number <tt>123</tt>, <tt>TRUE</tt>/<tt>FALSE</tt>, column <tt>$col$</tt></li>
              <li>Comments: lines starting with <tt>//</tt> are ignored</li>
              <li>Flow variables: <tt>$${SFlowVar}$$</tt>, <tt>$${IFlowVar}$$</tt>, <tt>$${DFlowVar}$$</tt></li>
            </ul>
            """)
        .modelSettingsClass(SparkRuleEngineNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to apply rules to.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the rule result column added or replaced.")
        .keywords("rule", "engine", "case", "when", "if", "then", "condition", "classify")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkRuleEngineNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkRuleEngineNodeModel createNodeModel() {
        return new SparkRuleEngineNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkRuleEngineNodeModel>> getNodeFactory() {
        return SparkRuleEngineNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkRuleEngineNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "sql";
    }

    @Override
    public String getAfterID() {
        return "";
    }

    @Override
    public ConfigRO getAdditionalSettings() {
        return null;
    }
}
