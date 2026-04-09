package org.knime.bigdata.spark.dx.node.preproc.constantvalue;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Constant Value Column node. Adds a new column with a constant value
 * or replaces an existing column with a constant value in a Spark DataFrame.
 */
@SuppressWarnings("restriction")
public final class SparkConstantValueColumnNodeFactory
    extends WebUINodeFactory<SparkConstantValueColumnNodeModel>
    implements SparkNodeFactory<SparkConstantValueColumnNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Constant Value Column (HYIM)")
        .icon("icon.png")
        .shortDescription("Adds or replaces a column with a constant value in a Spark DataFrame.")
        .fullDescription("<p>Adds a new column filled with a constant value, or replaces an existing column "
            + "with a constant value in a Spark DataFrame.</p>"
            + "<p>Supported value types: String, Integer, Long, Double, Boolean, Date, and Timestamp. "
            + "The column can also be filled with missing (null) values of the selected type.</p>")
        .modelSettingsClass(SparkConstantValueColumnNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to add or replace a constant value column.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the constant value column added or replaced.")
        .keywords("constant", "value", "column", "literal", "fill", "add")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkConstantValueColumnNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkConstantValueColumnNodeModel createNodeModel() {
        return new SparkConstantValueColumnNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkConstantValueColumnNodeModel>> getNodeFactory() {
        return SparkConstantValueColumnNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkConstantValueColumnNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "preproc";
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
