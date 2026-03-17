package org.knime.bigdata.spark.dx.node.preproc.duplicates;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Duplicate Row Filter node. Removes or annotates
 * duplicate rows using Spark Window Functions.
 */
@SuppressWarnings("restriction")
public final class SparkDuplicateRowFilterNodeFactory
    extends WebUINodeFactory<SparkDuplicateRowFilterNodeModel>
    implements SparkNodeFactory<SparkDuplicateRowFilterNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Duplicate Row Filter (Hyim)")
        .icon("icon.png")
        .shortDescription("Removes or annotates duplicate rows in a Spark DataFrame.")
        .fullDescription("""
            <p>Detects and handles duplicate rows in a Spark DataFrame based on selected columns.
            Two modes are available:</p>
            <ul>
              <li><b>Remove duplicates:</b> Keeps only one representative row per duplicate group
                  (first, last, minimum, maximum) or removes all duplicates entirely.</li>
              <li><b>Keep duplicates (annotate):</b> Retains all rows and optionally adds a status
                  column indicating whether each row is "unique", "chosen", or "duplicate".</li>
            </ul>
            <p>Uses Spark Window Functions (ROW_NUMBER, COUNT) for efficient distributed processing.</p>
            """)
        .modelSettingsClass(SparkDuplicateRowFilterNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with potential duplicate rows.")
        .addOutputPort("Output Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with duplicates removed or annotated.")
        .keywords("duplicate", "filter", "remove", "unique", "dedup", "deduplicate")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkDuplicateRowFilterNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkDuplicateRowFilterNodeModel createNodeModel() {
        return new SparkDuplicateRowFilterNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkDuplicateRowFilterNodeModel>> getNodeFactory() {
        return SparkDuplicateRowFilterNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkDuplicateRowFilterNodeFactory.class.getName();
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
