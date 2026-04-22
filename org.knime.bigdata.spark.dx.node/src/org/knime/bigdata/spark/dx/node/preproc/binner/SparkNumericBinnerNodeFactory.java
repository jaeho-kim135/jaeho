package org.knime.bigdata.spark.dx.node.preproc.binner;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Numeric Binner node. Bins numeric column values
 * into String categories using CASE WHEN expressions in Spark SQL.
 */
@SuppressWarnings("restriction")
public final class SparkNumericBinnerNodeFactory
    extends WebUINodeFactory<SparkNumericBinnerNodeModel>
    implements SparkNodeFactory<SparkNumericBinnerNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Numeric Binner (Obzen)")
        .icon("icon.png")
        .shortDescription("Bins numeric column values into categories in a Spark DataFrame.")
        .fullDescription(
            "<p>Bins (discretizes) numeric column values into categorical string labels\n"
            + "using Spark SQL CASE WHEN expressions.</p>\n"
            + "<p>Three binning modes are available:</p>\n"
            + "<ul>\n"
            + "    <li><b>Equal width</b> - Automatically divides the data range into equal-width intervals.</li>\n"
            + "    <li><b>Equal frequency</b> - Automatically creates bins with approximately equal row counts\n"
            + "        using quantile estimation.</li>\n"
            + "    <li><b>Custom ranges</b> - Manually define bin boundaries with inclusive/exclusive flags.</li>\n"
            + "</ul>\n"
            + "<p>Output columns are always of String type. The original numeric column can be\n"
            + "replaced or a new column can be appended with a configurable suffix.</p>")
        .modelSettingsClass(SparkNumericBinnerNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with numeric columns to bin.")
        .addOutputPort("Binned Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with binned (categorical) columns.")
        .keywords("bin", "binner", "discretize", "bucket", "category", "numeric", "range")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkNumericBinnerNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkNumericBinnerNodeModel createNodeModel() {
        return new SparkNumericBinnerNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkNumericBinnerNodeModel>> getNodeFactory() {
        return SparkNumericBinnerNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkNumericBinnerNodeFactory.class.getName();
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
