package org.knime.bigdata.spark.dx.node.preproc.numbertostring;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Number to String node. Converts numeric columns to
 * String type using Spark DataFrame operations.
 */
@SuppressWarnings("restriction")
public final class SparkNumberToStringNodeFactory
    extends WebUINodeFactory<SparkNumberToStringNodeModel>
    implements SparkNodeFactory<SparkNumberToStringNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Number to String(HYIM)")
        .icon("number_string.png")
        .shortDescription("Converts numeric columns to String type in a Spark DataFrame.")
        .fullDescription(
            "<p>Converts selected numeric columns (Integer, Long, Double) in a Spark DataFrame\n"
            + "to String type. Null values remain null in the output.</p>")
        .modelSettingsClass(SparkNumberToStringNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing numeric columns to convert.")
        .addOutputPort("Converted Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with selected columns converted to String type.")
        .build();

    /** Default constructor. */
    public SparkNumberToStringNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkNumberToStringNodeModel createNodeModel() {
        return new SparkNumberToStringNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkNumberToStringNodeModel>> getNodeFactory() {
        return SparkNumberToStringNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkNumberToStringNodeFactory.class.getName();
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
