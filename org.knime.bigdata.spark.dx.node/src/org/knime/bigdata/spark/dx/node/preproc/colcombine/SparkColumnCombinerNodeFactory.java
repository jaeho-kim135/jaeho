package org.knime.bigdata.spark.dx.node.preproc.colcombine;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Column Combiner node. Combines multiple columns
 * into a single string column using a configurable delimiter.
 */
@SuppressWarnings("restriction")
public final class SparkColumnCombinerNodeFactory
    extends WebUINodeFactory<SparkColumnCombinerNodeModel>
    implements SparkNodeFactory<SparkColumnCombinerNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Column Combiner (Hyim)")
        .icon("icon.png")
        .shortDescription("Combines multiple columns into a single string column using a delimiter.")
        .fullDescription("""
            <p>Combines the values of multiple columns into a single string column using
            Spark's CONCAT_WS function (requires Spark 3.4+).</p>
            <p>Select the columns to combine, specify a delimiter, and configure how
            missing values and delimiter occurrences within cells should be handled.</p>
            """)
        .modelSettingsClass(SparkColumnCombinerNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with columns to combine.")
        .addOutputPort("Combined Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the combined string column.")
        .keywords("combine", "column", "concatenate", "merge", "string", "join")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkColumnCombinerNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkColumnCombinerNodeModel createNodeModel() {
        return new SparkColumnCombinerNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkColumnCombinerNodeModel>> getNodeFactory() {
        return SparkColumnCombinerNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkColumnCombinerNodeFactory.class.getName();
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
