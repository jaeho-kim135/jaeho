package org.knime.bigdata.spark.dx.node.preproc.stringreplacer;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark String Replacer node. Replaces strings in a column
 * using literal, wildcard, or regular expression matching.
 */
@SuppressWarnings("restriction")
public final class SparkStringReplacerNodeFactory
    extends WebUINodeFactory<SparkStringReplacerNodeModel>
    implements SparkNodeFactory<SparkStringReplacerNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark String Replacer (HYIM)")
        .icon("icon.png")
        .shortDescription("Replaces strings in a column of a Spark DataFrame.")
        .fullDescription("<p>Replaces occurrences of a search pattern in a string column of a Spark DataFrame. "
            + "Supports literal text matching, wildcard patterns (* and ?), and regular expressions.</p>"
            + "<p>The replacement can target all occurrences within each cell or only cells "
            + "where the entire string matches the pattern. Case-sensitive and case-insensitive "
            + "matching are supported for all pattern types.</p>"
            + "<p>The result can either replace the original column or be appended as a new column. "
            + "The output column is always of type String.</p>")
        .modelSettingsClass(SparkStringReplacerNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing the string column to process.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the replaced string column.")
        .keywords("string", "replace", "regex", "wildcard", "pattern", "substitute", "find")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkStringReplacerNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkStringReplacerNodeModel createNodeModel() {
        return new SparkStringReplacerNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkStringReplacerNodeModel>> getNodeFactory() {
        return SparkStringReplacerNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkStringReplacerNodeFactory.class.getName();
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
