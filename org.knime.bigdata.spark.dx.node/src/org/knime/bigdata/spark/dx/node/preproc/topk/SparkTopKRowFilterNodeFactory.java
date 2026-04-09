package org.knime.bigdata.spark.dx.node.preproc.topk;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Top k Row Filter node. Selects the top k rows
 * from a Spark DataFrame based on one or two sort criteria, optionally per group.
 */
@SuppressWarnings("restriction")
public final class SparkTopKRowFilterNodeFactory
    extends WebUINodeFactory<SparkTopKRowFilterNodeModel>
    implements SparkNodeFactory<SparkTopKRowFilterNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Top k Row Filter (HYIM)")
        .icon("icon.png")
        .shortDescription("Selects the top k rows from a Spark DataFrame based on sorting criteria.")
        .fullDescription(
            "<p>Selects the top k rows from a Spark DataFrame based on one or two sort criteria.\n"
            + "Supports two filter modes:</p>\n"
            + "<ul>\n"
            + "  <li><b>Top k rows</b>: Returns exactly the top k rows.</li>\n"
            + "  <li><b>Top k unique values</b>: Returns all rows matching the top k distinct\n"
            + "      sort-column value combinations.</li>\n"
            + "</ul>\n"
            + "<p>Optionally, rows can be grouped by one or more columns so that the top k selection\n"
            + "is performed independently within each group (using Spark window functions).</p>\n"
            + "<p>Requires Spark 3.4+.</p>")
        .modelSettingsClass(SparkTopKRowFilterNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to filter.")
        .addOutputPort("Top k Rows", SparkDataPortObject.TYPE,
            "Spark DataFrame containing the top k rows.")
        .keywords("top", "topk", "filter", "rank", "sort", "select", "best", "worst")
        .sinceVersion(1, 0, 0)
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkTopKRowFilterNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkTopKRowFilterNodeModel createNodeModel() {
        return new SparkTopKRowFilterNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkTopKRowFilterNodeModel>> getNodeFactory() {
        return SparkTopKRowFilterNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkTopKRowFilterNodeFactory.class.getName();
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
