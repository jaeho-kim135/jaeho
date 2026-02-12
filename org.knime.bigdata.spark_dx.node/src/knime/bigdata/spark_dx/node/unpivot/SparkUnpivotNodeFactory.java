package knime.bigdata.spark_dx.node.unpivot;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeModel;

public final class SparkUnpivotNodeFactory extends NodeFactory<NodeModel> {

    @Override
    public NodeModel createNodeModel() {
        return new SparkUnpivotNodeModel();
    }

    @Override
    public int getNrNodeViews() {
        return 0;
    }

    @Override
    public org.knime.core.node.NodeView<NodeModel> createNodeView(final int viewIndex, final NodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new SparkUnpivotNodeDialog();
    }
}
