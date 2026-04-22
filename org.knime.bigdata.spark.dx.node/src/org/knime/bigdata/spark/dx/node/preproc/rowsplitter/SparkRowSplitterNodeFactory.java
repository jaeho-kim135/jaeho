package org.knime.bigdata.spark.dx.node.preproc.rowsplitter;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Row Splitter node. Splits rows into two output ports
 * based on configurable filter conditions.
 *
 * <p>Rows matching the filter conditions are sent to the first output port (Matches),
 * while non-matching rows are sent to the second output port (Non-Matches).
 */
@SuppressWarnings("restriction")
public final class SparkRowSplitterNodeFactory
    extends WebUINodeFactory<SparkRowSplitterNodeModel>
    implements SparkNodeFactory<SparkRowSplitterNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Row Splitter (Obzen)")
        .icon("icon.png")
        .shortDescription("Splits rows of a Spark DataFrame into two outputs based on filter conditions.")
        .fullDescription("<p>Splits the input Spark DataFrame into two disjoint sets based on one or more "
            + "filter conditions. Rows that satisfy the conditions are sent to the first output "
            + "port (Matches), and rows that do not satisfy the conditions are sent to the second "
            + "output port (Non-Matches).</p>"
            + "<p>Multiple filter predicates can be combined using AND (all conditions must match) "
            + "or OR (any condition must match) logic. Each predicate specifies a column, a "
            + "comparison operator, and a value to compare against.</p>"
            + "<p>Supported operators include equality, inequality, comparison, BETWEEN, LIKE, "
            + "REGEX, IS NULL, IS NOT NULL, IS TRUE, and IS FALSE. String comparisons can be "
            + "configured as case-sensitive or case-insensitive.</p>")
        .modelSettingsClass(SparkRowSplitterNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to split.")
        .addOutputPort("Matches", SparkDataPortObject.TYPE,
            "Rows that match the filter conditions.")
        .addOutputPort("Non-Matches", SparkDataPortObject.TYPE,
            "Rows that do not match the filter conditions.")
        .keywords("row", "splitter", "split", "filter", "condition", "partition")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkRowSplitterNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkRowSplitterNodeModel createNodeModel() {
        return new SparkRowSplitterNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkRowSplitterNodeModel>> getNodeFactory() {
        return SparkRowSplitterNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkRowSplitterNodeFactory.class.getName();
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
