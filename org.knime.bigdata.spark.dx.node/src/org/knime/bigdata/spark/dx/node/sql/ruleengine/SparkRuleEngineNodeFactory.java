package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;

/**
 * Node factory for the Spark Rule Engine node. Converts IF-THEN rules
 * to Spark SQL CASE WHEN expressions for row-level classification.
 *
 * <p>Implements {@link NodeDialogFactory} to provide a modern WebUI dialog
 * with column list, flow variables, function catalog, and output preview.
 */
@SuppressWarnings("restriction")
public final class SparkRuleEngineNodeFactory extends DefaultSparkNodeFactory<SparkRuleEngineNodeModel>
    implements NodeDialogFactory {

    /** Default constructor. */
    public SparkRuleEngineNodeFactory() {
        super("sql");
    }

    @Override
    public SparkRuleEngineNodeModel createNodeModel() {
        return new SparkRuleEngineNodeModel();
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new SparkRuleEngineWebNodeDialog();
    }
}
