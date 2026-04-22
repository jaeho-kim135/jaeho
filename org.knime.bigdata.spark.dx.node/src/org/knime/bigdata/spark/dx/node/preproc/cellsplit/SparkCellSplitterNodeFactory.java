package org.knime.bigdata.spark.dx.node.preproc.cellsplit;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Cell Splitter node. Splits a string column into
 * multiple columns by a delimiter using Spark's {@code split()} function.
 */
@SuppressWarnings("restriction")
public final class SparkCellSplitterNodeFactory
    extends WebUINodeFactory<SparkCellSplitterNodeModel>
    implements SparkNodeFactory<SparkCellSplitterNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Cell Splitter (Obzen)")
        .icon("icon.png")
        .shortDescription("Splits a string column into multiple columns by a delimiter.")
        .fullDescription("<p>Splits the content of a string column into multiple new columns using a delimiter. "
            + "The delimiter can be a literal string or a regular expression.</p>"
            + "<p>The number of output columns can be set to a fixed value or auto-detected from "
            + "the data by scanning a configurable number of rows to determine the maximum number "
            + "of split parts.</p>"
            + "<p>Options include trimming whitespace from split results, replacing null values "
            + "with empty strings, removing the original input column, and specifying a custom "
            + "output column name prefix.</p>")
        .modelSettingsClass(SparkCellSplitterNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame containing the string column to split.")
        .addOutputPort("Split Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the split columns appended.")
        .keywords("cell", "split", "delimiter", "string", "column", "separate", "tokenize")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkCellSplitterNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkCellSplitterNodeModel createNodeModel() {
        return new SparkCellSplitterNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkCellSplitterNodeModel>> getNodeFactory() {
        return SparkCellSplitterNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkCellSplitterNodeFactory.class.getName();
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
