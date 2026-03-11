package org.knime.bigdata.spark.dx.node.preproc.stringtodatetime;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark String to Date&Time node. Converts string columns to
 * date/time types using Spark DataFrame operations.
 */
@SuppressWarnings("restriction")
public final class SparkStringToDateTimeNodeFactory
    extends WebUINodeFactory<SparkStringToDateTimeNodeModel>
    implements SparkNodeFactory<SparkStringToDateTimeNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark String to Date&Time(Hyim)")
        .icon("stringtotime.png")
        .shortDescription("Parses date and/or time strings into date&time columns in a Spark DataFrame.")
        .fullDescription("""
            <p>Parses strings in selected columns and converts them to date&amp;time columns \
            using Spark DataFrame operations. You can choose the output type (Date, Time, \
            Date&amp;Time, or Zoned Date&amp;Time) and specify a format pattern.</p>
            <p>The format pattern uses Java DateTimeFormatter syntax. Common examples:</p>
            <ul>
                <li>"yyyy-MM-dd" for dates like "2024-01-15"</li>
                <li>"HH:mm:ss" for times like "14:30:00"</li>
                <li>"yyyy-MM-dd HH:mm:ss" for date&amp;time like "2024-01-15 14:30:00"</li>
                <li>"yyyy-MM-dd'T'HH:mm:ss.SSSXXX" for ISO format with timezone offset</li>
            </ul>
            <p><b>Note:</b> Spark's date parsing supports most common DateTimeFormatter patterns. \
            Some advanced patterns (e.g., timezone IDs with 'VV') may have limited support.</p>
            """)
        .modelSettingsClass(SparkStringToDateTimeNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing string columns to convert.")
        .addOutputPort("Converted Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with selected columns converted to date&time type.")
        .keywords("convert", "date", "time", "string", "parse", "spark", "datetime", "timestamp")
        .build();

    /** Default constructor. */
    public SparkStringToDateTimeNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkStringToDateTimeNodeModel createNodeModel() {
        return new SparkStringToDateTimeNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkStringToDateTimeNodeModel>> getNodeFactory() {
        return SparkStringToDateTimeNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkStringToDateTimeNodeFactory.class.getName();
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
