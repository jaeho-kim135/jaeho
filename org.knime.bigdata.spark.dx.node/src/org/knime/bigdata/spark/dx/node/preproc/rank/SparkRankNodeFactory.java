package org.knime.bigdata.spark.dx.node.preproc.rank;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Rank node. Computes rank values using
 * Spark SQL window functions (RANK, DENSE_RANK, ROW_NUMBER).
 */
@SuppressWarnings("restriction")
public final class SparkRankNodeFactory
    extends WebUINodeFactory<SparkRankNodeModel>
    implements SparkNodeFactory<SparkRankNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Rank (Hyim)")
        .icon("icon.png")
        .shortDescription("Assigns rank values to rows of a Spark DataFrame using window functions.")
        .fullDescription("""
            <p>Assigns rank values to rows of a Spark DataFrame based on the ordering of one or more
            columns, using Spark SQL window functions (requires Spark 3.4+).</p>
            <p>Supports three ranking modes:</p>
            <ul>
              <li><b>Standard (RANK)</b>: Tied rows get the same rank; the next distinct value
                  gets a rank incremented by the number of ties (e.g., 1, 1, 3, 4).</li>
              <li><b>Dense (DENSE_RANK)</b>: Tied rows get the same rank; the next distinct value
                  gets a rank incremented by one (e.g., 1, 1, 2, 3).</li>
              <li><b>Ordinal (ROW_NUMBER)</b>: Each row gets a unique rank, even for ties
                  (e.g., 1, 2, 3, 4).</li>
            </ul>
            <p>Optionally, ranking can be partitioned by group columns so that ranks are computed
            independently within each group.</p>
            """)
        .modelSettingsClass(SparkRankNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to rank.")
        .addOutputPort("Ranked Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with an appended rank column.")
        .keywords("rank", "ranking", "order", "window", "dense", "row_number")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkRankNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkRankNodeModel createNodeModel() {
        return new SparkRankNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkRankNodeModel>> getNodeFactory() {
        return SparkRankNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkRankNodeFactory.class.getName();
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
