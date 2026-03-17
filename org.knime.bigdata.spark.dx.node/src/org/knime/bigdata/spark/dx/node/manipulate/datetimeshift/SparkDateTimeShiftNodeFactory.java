package org.knime.bigdata.spark.dx.node.manipulate.datetimeshift;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Date&amp;Time Shift node. Shifts date/time columns
 * by a fixed or column-based value using Spark SQL functions.
 */
@SuppressWarnings("restriction")
public final class SparkDateTimeShiftNodeFactory
    extends WebUINodeFactory<SparkDateTimeShiftNodeModel>
    implements SparkNodeFactory<SparkDateTimeShiftNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Date&Time Shift (Hyim)")
        .icon("icon.png")
        .shortDescription("Shifts date/time columns by a fixed or column-based value.")
        .fullDescription("""
            <p>Shifts one or more date/time columns forward or backward by a specified amount
            using Spark SQL functions (DATE_ADD, ADD_MONTHS, MAKE_INTERVAL).</p>
            <p>The shift can be a fixed integer value or taken from a column per row.
            Supported granularities: Years, Months, Weeks, Days, Hours, Minutes, Seconds,
            and Milliseconds.</p>
            <p>The result can replace the original columns or be appended as new columns
            with a configurable suffix.</p>
            <p><b>Note:</b> Shifting a Date column by Hours/Minutes/Seconds/Milliseconds
            may change the output type to Timestamp.</p>
            """)
        .modelSettingsClass(SparkDateTimeShiftNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing date/time columns to shift.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with shifted date/time columns.")
        .keywords("date", "time", "shift", "add", "subtract", "interval", "offset",
            "day", "month", "year", "hour", "minute", "second")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkDateTimeShiftNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkDateTimeShiftNodeModel createNodeModel() {
        return new SparkDateTimeShiftNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkDateTimeShiftNodeModel>> getNodeFactory() {
        return SparkDateTimeShiftNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkDateTimeShiftNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "manipulate";
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
