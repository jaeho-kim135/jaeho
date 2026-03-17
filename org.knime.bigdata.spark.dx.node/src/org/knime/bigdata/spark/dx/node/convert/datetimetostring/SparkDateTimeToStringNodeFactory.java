package org.knime.bigdata.spark.dx.node.convert.datetimetostring;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Date&amp;Time to String node. Converts date/time columns
 * to string columns using Spark's {@code date_format()} function.
 */
@SuppressWarnings("restriction")
public final class SparkDateTimeToStringNodeFactory
    extends WebUINodeFactory<SparkDateTimeToStringNodeModel>
    implements SparkNodeFactory<SparkDateTimeToStringNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Date&Time to String (Hyim)")
        .icon("icon.png")
        .shortDescription("Converts date/time columns to string columns using Spark's date_format().")
        .fullDescription("""
            <p>Converts one or more date/time columns of a Spark DataFrame to string columns
            using Spark's <tt>date_format()</tt> function.</p>
            <p>The format pattern follows Java/Spark's SimpleDateFormat convention
            (e.g., <tt>yyyy-MM-dd HH:mm:ss</tt>). An optional locale can be specified
            for locale-sensitive patterns (month/day names).</p>
            <p>Selected columns can either be replaced in-place or appended as new columns
            with a configurable suffix.</p>
            """)
        .modelSettingsClass(SparkDateTimeToStringNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing date/time columns to convert.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the selected date/time columns converted to string.")
        .keywords("date", "time", "string", "format", "convert", "datetime")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkDateTimeToStringNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkDateTimeToStringNodeModel createNodeModel() {
        return new SparkDateTimeToStringNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkDateTimeToStringNodeModel>> getNodeFactory() {
        return SparkDateTimeToStringNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkDateTimeToStringNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "convert";
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
