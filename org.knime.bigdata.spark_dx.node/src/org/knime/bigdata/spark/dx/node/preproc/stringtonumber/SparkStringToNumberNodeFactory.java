package org.knime.bigdata.spark.dx.node.preproc.stringtonumber;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark String to Number node. Converts String columns to
 * numeric types (Integer, Double, Long) using Spark DataFrame operations.
 */
@SuppressWarnings("restriction")
public final class SparkStringToNumberNodeFactory
    extends WebUINodeFactory<SparkStringToNumberNodeModel>
    implements SparkNodeFactory<SparkStringToNumberNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark String to Number(Hyim)")
        .icon("string_number.png")
        .shortDescription("Converts String columns to numeric types in a Spark DataFrame.")
        .fullDescription("""
            <p>Converts selected String columns in a Spark DataFrame to numeric types
            (Integer, Double, or Long). Supports configurable decimal and thousands
            separators, optional type suffix handling (d/D/f/F), and error handling.</p>
            <p>Values that cannot be parsed are set to missing (null) by default.
            Enable <b>Fail on error</b> to abort execution when conversion fails.</p>
            """)
        .modelSettingsClass(SparkStringToNumberNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing String columns to convert.")
        .addOutputPort("Converted Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with selected columns converted to numeric types.")
        .build();

    /** Default constructor. */
    public SparkStringToNumberNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkStringToNumberNodeModel createNodeModel() {
        return new SparkStringToNumberNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkStringToNumberNodeModel>> getNodeFactory() {
        return SparkStringToNumberNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkStringToNumberNodeFactory.class.getName();
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
