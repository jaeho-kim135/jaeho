package org.knime.bigdata.spark.dx.node.preproc.transpose;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Table Transposer node. Transposes a Spark DataFrame
 * by converting rows to columns and columns to rows.
 */
@SuppressWarnings("restriction")
public final class SparkTransposeNodeFactory
    extends WebUINodeFactory<SparkTransposeNodeModel>
    implements SparkNodeFactory<SparkTransposeNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Table Transposer (Obzen)")
        .icon("icon.png")
        .shortDescription("Transposes a Spark DataFrame (rows become columns and vice versa).")
        .fullDescription("<p>Transposes a Spark DataFrame by converting rows to columns and columns to rows. "
            + "<b>WARNING: This node collects all data to the driver. Only suitable for small datasets.</b></p>"
            + "<p>All values are converted to String type in the transposed output.</p>")
        .modelSettingsClass(SparkTransposeNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE, "Spark DataFrame to transpose.")
        .addOutputPort("Transposed Data", SparkDataPortObject.TYPE, "Transposed DataFrame.")
        .keywords("transpose", "pivot", "rotate", "flip", "rows to columns")
        .sinceVersion(1, 0, 0)
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkTransposeNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkTransposeNodeModel createNodeModel() {
        return new SparkTransposeNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkTransposeNodeModel>> getNodeFactory() {
        return SparkTransposeNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkTransposeNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "row";
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
