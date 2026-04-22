package org.knime.bigdata.spark.dx.node.preproc.caseconvert;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Case Converter node. Converts the case of selected
 * string columns in a Spark DataFrame using UPPER(), LOWER(), or INITCAP().
 */
@SuppressWarnings("restriction")
public final class SparkCaseConvertNodeFactory
    extends WebUINodeFactory<SparkCaseConvertNodeModel>
    implements SparkNodeFactory<SparkCaseConvertNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Case Converter (Obzen)")
        .icon("icon.png")
        .shortDescription("Converts the case of string columns in a Spark DataFrame.")
        .fullDescription(
            "<p>Converts the case of selected string columns in a Spark DataFrame.\n"
            + "Supports UPPERCASE, lowercase, and Title Case conversion using\n"
            + "Spark SQL functions UPPER(), LOWER(), and INITCAP().</p>\n"
            + "<p>Only String type columns are available for selection.\n"
            + "The conversion is always applied in-place (replacing the original column values).</p>")
        .modelSettingsClass(SparkCaseConvertNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with string columns to convert.")
        .addOutputPort("Converted Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with converted case.")
        .keywords("case", "upper", "lower", "title", "convert", "string")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkCaseConvertNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkCaseConvertNodeModel createNodeModel() {
        return new SparkCaseConvertNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkCaseConvertNodeModel>> getNodeFactory() {
        return SparkCaseConvertNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkCaseConvertNodeFactory.class.getName();
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
