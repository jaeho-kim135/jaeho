package org.knime.bigdata.spark.dx.node.preproc.rounddouble;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Number Rounder node. Rounds numeric columns using various
 * rounding methods including ROUND, CEIL, FLOOR and BigDecimal-based rounding.
 */
@SuppressWarnings("restriction")
public final class SparkRoundDoubleNodeFactory
    extends WebUINodeFactory<SparkRoundDoubleNodeModel>
    implements SparkNodeFactory<SparkRoundDoubleNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Number Rounder (Hyim)")
        .icon("icon.png")
        .shortDescription("Rounds numeric columns in a Spark DataFrame using configurable rounding methods.")
        .fullDescription("""
            <p>Rounds the values of selected numeric columns in a Spark DataFrame.</p>
            <p>Supports three number modes:</p>
            <ul>
              <li><b>Decimal places</b>: Round to a specified number of decimal places.</li>
              <li><b>Significant digits</b>: Round to a specified number of significant digits.</li>
              <li><b>Integer</b>: Round to whole numbers (result cast to Long type).</li>
            </ul>
            <p>Supports multiple rounding methods:</p>
            <ul>
              <li>Standard (.5 rounds up) - uses Spark's built-in ROUND function</li>
              <li>Round up (away from zero)</li>
              <li>Round down (towards zero)</li>
              <li>Round ceiling (+infinity direction)</li>
              <li>Round floor (-infinity direction)</li>
              <li>Half down (.5 towards zero)</li>
              <li>Half even (banker's rounding)</li>
            </ul>
            <p>The result can either replace the original columns or be appended as new columns
            with a configurable suffix.</p>
            """)
        .modelSettingsClass(SparkRoundDoubleNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with numeric columns to round.")
        .addOutputPort("Rounded Data", SparkDataPortObject.TYPE,
            "Spark DataFrame with rounded numeric columns.")
        .keywords("round", "double", "decimal", "precision", "ceil", "floor", "truncate",
            "significant", "banker", "number")
        .nodeType(NodeType.Manipulator)
        .build();

    /** Default constructor. */
    public SparkRoundDoubleNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkRoundDoubleNodeModel createNodeModel() {
        return new SparkRoundDoubleNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkRoundDoubleNodeModel>> getNodeFactory() {
        return SparkRoundDoubleNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkRoundDoubleNodeFactory.class.getName();
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
