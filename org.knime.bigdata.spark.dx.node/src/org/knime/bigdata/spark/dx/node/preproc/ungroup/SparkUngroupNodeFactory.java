package org.knime.bigdata.spark.dx.node.preproc.ungroup;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Ungroup node. Explodes array, map, or delimited string
 * columns into individual rows using Spark's explode() function.
 */
@SuppressWarnings("restriction")
public final class SparkUngroupNodeFactory
    extends WebUINodeFactory<SparkUngroupNodeModel>
    implements SparkNodeFactory<SparkUngroupNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Ungroup (Obzen)")
        .icon("icon.png")
        .shortDescription("Explodes array, map, or delimited string columns into individual rows.")
        .fullDescription(
            "<p>Ungroups (explodes) selected columns of a Spark DataFrame. Each element in an array\n"
            + "or map column becomes a separate row. String columns can be split by a delimiter before\n"
            + "exploding.</p>\n"
            + "<p>Two modes are supported:</p>\n"
            + "<ul>\n"
            + "  <li><b>Auto-detect:</b> Automatically detects ArrayType/MapType columns and applies\n"
            + "      explode(). StringType columns are split by the specified delimiter.</li>\n"
            + "  <li><b>Split string by delimiter:</b> Treats all selected columns as strings, splits\n"
            + "      them by the specified delimiter, and explodes the resulting arrays.</li>\n"
            + "</ul>\n"
            + "<p>Requires Spark 3.4+.</p>")
        .modelSettingsClass(SparkUngroupNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with array, map, or string columns to ungroup.")
        .addOutputPort("Ungrouped Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with exploded rows.")
        .keywords("ungroup", "explode", "split", "array", "map", "flatten")
        .sinceVersion(1, 0, 0)
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkUngroupNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkUngroupNodeModel createNodeModel() {
        return new SparkUngroupNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkUngroupNodeModel>> getNodeFactory() {
        return SparkUngroupNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkUngroupNodeFactory.class.getName();
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
