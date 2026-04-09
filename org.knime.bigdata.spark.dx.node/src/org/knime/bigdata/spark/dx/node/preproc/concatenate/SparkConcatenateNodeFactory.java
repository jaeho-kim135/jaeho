package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;

/**
 * Node factory for the Spark Concatenate node. Vertically concatenates two Spark DataFrames
 * with configurable column mapping and type resolution.
 *
 * <p>Implements {@link NodeDialogFactory} to provide a modern WebUI dialog
 * with column mapping table, auto-mapping, and unmatched column handling.
 */
@SuppressWarnings("restriction")
public final class SparkConcatenateNodeFactory extends DefaultSparkNodeFactory<SparkConcatenateNodeModel>
    implements NodeDialogFactory {

    /** Default constructor. */
    public SparkConcatenateNodeFactory() {
        super("preproc");
    }

    @Override
    public SparkConcatenateNodeModel createNodeModel() {
        return new SparkConcatenateNodeModel();
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
        return new SparkConcatenateWebNodeDialog();
    }
}
