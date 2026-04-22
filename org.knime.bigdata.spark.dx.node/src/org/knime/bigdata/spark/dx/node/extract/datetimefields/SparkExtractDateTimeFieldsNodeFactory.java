package org.knime.bigdata.spark.dx.node.extract.datetimefields;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Extract Date&amp;Time Fields node.
 * Extracts year, month, day, hour, minute, second, and other date/time components
 * from a date/time column in a Spark DataFrame.
 */
@SuppressWarnings("restriction")
public final class SparkExtractDateTimeFieldsNodeFactory
    extends WebUINodeFactory<SparkExtractDateTimeFieldsNodeModel>
    implements SparkNodeFactory<SparkExtractDateTimeFieldsNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Extract Date&Time Fields (Obzen)")
        .icon("icon.png")
        .shortDescription("Extracts date and time fields (year, month, day, hour, etc.) from a date/time column.")
        .fullDescription(
            "<p>Extracts individual date and time fields from a date, timestamp, or time column\n"
            + "in a Spark DataFrame. Supported fields include:</p>\n"
            + "<ul>\n"
            + "    <li><b>Date fields:</b> Year, Month (number), Day of month, Day of week (number),\n"
            + "        Day of year, Week of year, Quarter</li>\n"
            + "    <li><b>Time fields:</b> Hour, Minute, Second</li>\n"
            + "    <li><b>Subsecond:</b> Millisecond, Microsecond, or Nanosecond</li>\n"
            + "    <li><b>Name fields:</b> Day of week name, Month name (locale-dependent)</li>\n"
            + "</ul>\n"
            + "<p>Each extracted field is appended as a new integer column (or string column for name fields).\n"
            + "An optional column name prefix can be specified.</p>")
        .modelSettingsClass(SparkExtractDateTimeFieldsNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing a date/time column.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with extracted date/time fields appended.")
        .keywords("extract", "date", "time", "year", "month", "day", "hour", "minute", "second",
            "quarter", "week", "dayofweek", "dayofyear", "timestamp")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkExtractDateTimeFieldsNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkExtractDateTimeFieldsNodeModel createNodeModel() {
        return new SparkExtractDateTimeFieldsNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkExtractDateTimeFieldsNodeModel>> getNodeFactory() {
        return SparkExtractDateTimeFieldsNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkExtractDateTimeFieldsNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "extract";
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
