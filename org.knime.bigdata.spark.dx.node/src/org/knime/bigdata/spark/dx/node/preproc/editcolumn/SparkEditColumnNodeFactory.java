package org.knime.bigdata.spark.dx.node.preproc.editcolumn;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;

/**
 * Node factory for the Spark Edit Column node. Renames columns, changes data types,
 * and reorders columns in a Spark DataFrame.
 *
 * <p>Implements {@link NodeDialogFactory} to provide a modern WebUI dialog
 * with column editing table, type casting, and reordering.
 */
@SuppressWarnings("restriction")
public final class SparkEditColumnNodeFactory extends DefaultSparkNodeFactory<SparkEditColumnNodeModel>
    implements NodeDialogFactory {

    /** Default constructor. */
    public SparkEditColumnNodeFactory() {
        super("preproc");
    }

    @Override
    public SparkEditColumnNodeModel createNodeModel() {
        return new SparkEditColumnNodeModel();
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
        return new SparkEditColumnWebNodeDialog();
    }
}
