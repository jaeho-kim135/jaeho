package org.knime.bigdata.spark.dx.node.sql.multiquery;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Multi Query node. Applies a SQL expression template
 * to each selected column, replacing the $columnS placeholder with the column name.
 */
@SuppressWarnings("restriction")
public final class SparkMultiQueryNodeFactory
    extends WebUINodeFactory<SparkMultiQueryNodeModel>
    implements SparkNodeFactory<SparkMultiQueryNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Multi Query (Obzen)")
        .icon("icon.png")
        .shortDescription("Applies a SQL expression to multiple selected columns.")
        .fullDescription(
            "<p>Applies a SQL expression template to each selected target column. The placeholder\n"
            + "<b>$columnS</b> in the expression is replaced with each target column name at execution time.</p>\n"
            + "<p>For example, with expression <tt>string($columnS)</tt> and target columns\n"
            + "<tt>age</tt> and <tt>salary</tt>, the node generates:\n"
            + "<tt>SELECT string(`age`) AS `age`, string(`salary`) AS `salary`, ... FROM input</tt></p>\n"
            + "<p>Use the <b>Output column pattern</b> to control output column naming:\n"
            + "<tt>$columnS</tt> replaces the original column (default), or <tt>$columnS_str</tt> adds a new column.</p>\n"
            + "<p>The <b>Keep original columns</b> option preserves the original target columns alongside\n"
            + "the transformed ones (requires a non-default output pattern to avoid duplicate column names).</p>")
        .modelSettingsClass(SparkMultiQueryNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to apply the SQL expression to.")
        .addOutputPort("Transformed Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with the SQL expression applied to the target columns.")
        .build();

    /** Default constructor. */
    public SparkMultiQueryNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkMultiQueryNodeModel createNodeModel() {
        return new SparkMultiQueryNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkMultiQueryNodeModel>> getNodeFactory() {
        return SparkMultiQueryNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkMultiQueryNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "sql";
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
