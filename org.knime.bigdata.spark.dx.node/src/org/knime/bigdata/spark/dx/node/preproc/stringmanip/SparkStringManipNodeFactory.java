package org.knime.bigdata.spark.dx.node.preproc.stringmanip;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark String Manipulation node. Applies a Spark SQL
 * string expression to produce a new or replaced column in a Spark DataFrame.
 */
@SuppressWarnings("restriction")
public final class SparkStringManipNodeFactory
    extends WebUINodeFactory<SparkStringManipNodeModel>
    implements SparkNodeFactory<SparkStringManipNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark String Manipulation (HYIM)")
        .icon("icon.png")
        .shortDescription("Applies a Spark SQL string expression to manipulate string columns.")
        .fullDescription(
            "<p>Applies a Spark SQL string expression to a Spark DataFrame column,\n"
            + "producing a new column or replacing an existing one. This node provides\n"
            + "access to Spark's built-in string functions via SQL expressions.</p>\n"
            + "\n"
            + "<p><b>Available Spark SQL string functions:</b></p>\n"
            + "<ul>\n"
            + "  <li><b>Case conversion:</b> initcap, lower, upper</li>\n"
            + "  <li><b>Trimming:</b> trim, ltrim, rtrim</li>\n"
            + "  <li><b>Length &amp; position:</b> length, reverse, substring, locate</li>\n"
            + "  <li><b>Replacement:</b> replace, regexp_replace, regexp_extract</li>\n"
            + "  <li><b>Padding:</b> lpad, rpad</li>\n"
            + "  <li><b>Concatenation:</b> concat, concat_ws</li>\n"
            + "  <li><b>Type conversion:</b> cast</li>\n"
            + "</ul>\n"
            + "\n"
            + "<p><b>Usage examples:</b></p>\n"
            + "<ul>\n"
            + "  <li><tt>upper(`name`)</tt> - Convert to uppercase</li>\n"
            + "  <li><tt>concat(`first_name`, ' ', `last_name`)</tt> - Concatenate columns</li>\n"
            + "  <li><tt>regexp_replace(`text`, '[0-9]', '')</tt> - Remove digits</li>\n"
            + "  <li><tt>substring(`code`, 1, 3)</tt> - Extract substring</li>\n"
            + "  <li><tt>lpad(`id`, 5, '0')</tt> - Left-pad with zeros</li>\n"
            + "  <li><tt>trim(`text`)</tt> - Remove leading/trailing whitespace</li>\n"
            + "</ul>\n"
            + "\n"
            + "<p>Use backtick-quoted column names (e.g. <tt>`column name`</tt>) to reference\n"
            + "input columns in the expression. Flow variable placeholders\n"
            + "(<tt>$${varName}</tt>) are resolved before execution.</p>")
        .modelSettingsClass(SparkStringManipNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to apply string manipulation to.")
        .addOutputPort("Manipulated Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the string manipulation result.")
        .keywords("string", "manipulation", "expression", "regex", "replace",
            "upper", "lower", "trim", "concat", "substring", "spark")
        .sinceVersion(1, 0, 0)
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkStringManipNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkStringManipNodeModel createNodeModel() {
        return new SparkStringManipNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkStringManipNodeModel>> getNodeFactory() {
        return SparkStringManipNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkStringManipNodeFactory.class.getName();
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
