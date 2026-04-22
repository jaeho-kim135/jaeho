package org.knime.bigdata.spark.dx.node.preproc.lagcolumn;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Lag Column node. Creates lag or lead columns
 * using Spark SQL Window functions (LAG/LEAD).
 */
@SuppressWarnings("restriction")
public final class SparkLagColumnNodeFactory
    extends WebUINodeFactory<SparkLagColumnNodeModel>
    implements SparkNodeFactory<SparkLagColumnNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Lag Column (Obzen)")
        .icon("lag_column.png")
        .shortDescription("Creates lag or lead columns using Spark Window functions.")
        .fullDescription(
            "<p>Creates one or more lag (previous row) or lead (next row) columns using Spark SQL\n"
            + "Window functions. Requires an order-by column since Spark DataFrames have no inherent\n"
            + "row order.</p>\n"
            + "<p>For each copy, the offset is calculated as: copy_number * interval. The new columns\n"
            + "are named 'column(-offset)' for lag or 'column(+offset)' for lead.</p>\n"
            + "<p>Optionally, rows with incomplete (null) lag values can be filtered out.</p>")
        .modelSettingsClass(SparkLagColumnNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame/RDD")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "DataFrame with lag/lead columns appended.")
        .keywords("lag", "lead", "window", "shift", "previous", "next", "offset")
        .sinceVersion(1, 0, 0)
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkLagColumnNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkLagColumnNodeModel createNodeModel() {
        return new SparkLagColumnNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkLagColumnNodeModel>> getNodeFactory() {
        return SparkLagColumnNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkLagColumnNodeFactory.class.getName();
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
