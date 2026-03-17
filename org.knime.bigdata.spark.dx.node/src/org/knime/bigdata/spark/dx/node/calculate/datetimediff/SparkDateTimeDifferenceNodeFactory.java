package org.knime.bigdata.spark.dx.node.calculate.datetimediff;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Date&Time Difference node.
 * Calculates the difference between two date/time values in a Spark DataFrame.
 */
@SuppressWarnings("restriction")
public final class SparkDateTimeDifferenceNodeFactory
    extends WebUINodeFactory<SparkDateTimeDifferenceNodeModel>
    implements SparkNodeFactory<SparkDateTimeDifferenceNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Date&Time Difference (Hyim)")
        .icon("icon.png")
        .shortDescription("Calculates the difference between two date/time values in a Spark DataFrame.")
        .fullDescription("""
            <p>Calculates the difference between two date/time values in a Spark DataFrame.
            The first value is always taken from a selected column. The second value can come from
            another column, a fixed date/time string, or the current timestamp.</p>
            <p>The result granularity can be configured to years, months, weeks, days, hours, minutes,
            seconds, milliseconds, or microseconds. The direction of the subtraction can also be chosen.</p>
            <p>For Years/Months: uses Spark's <code>months_between()</code> function.<br/>
            For Weeks/Days: uses Spark's <code>datediff()</code> function.<br/>
            For Hours/Minutes/Seconds/Milliseconds/Microseconds: uses Spark's <code>unix_timestamp()</code>,
            <code>unix_millis()</code>, or <code>unix_micros()</code> functions.</p>
            """)
        .modelSettingsClass(SparkDateTimeDifferenceNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing date/time columns.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the computed difference column appended.")
        .keywords("date", "time", "difference", "subtract", "duration", "interval", "datediff")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkDateTimeDifferenceNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkDateTimeDifferenceNodeModel createNodeModel() {
        return new SparkDateTimeDifferenceNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkDateTimeDifferenceNodeModel>> getNodeFactory() {
        return SparkDateTimeDifferenceNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkDateTimeDifferenceNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "calculate";
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
