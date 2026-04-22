package org.knime.bigdata.spark.dx.node.sql.multiquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Node model for the Spark Multi Query node. Applies a SQL expression template
 * to each selected column, replacing the $columnS placeholder with the column name.
 * Flow variables ($$varName) in the expression are resolved to their values before execution.
 */
public class SparkMultiQueryNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkMultiQueryNodeModel.class.getCanonicalName();

    /**
     * Pattern matching $$varName tokens in SQL expressions.
     * $$  — double-dollar prefix distinguishes flow variables from the $columnS column placeholder.
     * Group 1 captures the variable name (letters, digits, underscores, hyphens, dots).
     */
    static final Pattern FLOW_VAR_PATTERN =
        Pattern.compile("\\$\\$([A-Za-z_][A-Za-z0-9_.\\-]*)");

    private final SparkMultiQuerySettings m_settings = new SparkMultiQuerySettings();

    /** Constructor. */
    public SparkMultiQueryNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame/RDD available");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        // Validate target columns (skip $$ flow variable references — resolved at execution)
        final List<String> targetColumns = m_settings.getTargetColumns();
        if (targetColumns == null || targetColumns.isEmpty()) {
            throw new InvalidSettingsException("No target columns selected. "
                + "Please select at least one column to apply the SQL expression to.");
        }

        for (String col : targetColumns) {
            if (col.startsWith("$$")) {
                continue; // flow variable reference — validated at execution
            }
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Target column '" + col + "' not found in input table.");
            }
        }

        // Validate SQL expression
        final String expr = m_settings.getSqlExpression();
        if (expr == null || expr.trim().isEmpty()) {
            throw new InvalidSettingsException("SQL expression must not be empty.");
        }

        if (!expr.contains(SparkMultiQuerySettings.COLUMN_PLACEHOLDER)) {
            throw new InvalidSettingsException(
                "SQL expression must contain the placeholder '" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER
                + "' which will be replaced with each target column name.");
        }

        // Validate output column pattern
        final String outputPattern = m_settings.getOutputColumnPattern();
        if (outputPattern == null || outputPattern.trim().isEmpty()) {
            throw new InvalidSettingsException("Output column pattern must not be empty.");
        }

        if (!outputPattern.contains(SparkMultiQuerySettings.COLUMN_PLACEHOLDER)) {
            throw new InvalidSettingsException(
                "Output column pattern must contain '" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + "'.");
        }

        // Output spec is null because the SQL expression may change column types
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> rawTargetColumns = m_settings.getTargetColumns();

        // Filter out $$flow-variable references (they are for expression substitution, not targets)
        final List<String> regularColumns = new ArrayList<>();
        for (final String col : rawTargetColumns) {
            if (!col.startsWith("$$")) {
                regularColumns.add(col);
            }
        }

        // Resolve $$varName tokens in SQL expression to actual flow variable values
        final String resolvedSql = resolveFlowVariables(m_settings.getSqlExpression());

        final SparkMultiQueryJobInput jobInput = new SparkMultiQueryJobInput(
            inputObject,
            outputObject,
            regularColumns.toArray(new String[0]),
            resolvedSql,
            m_settings.keepOriginalColumns(),
            m_settings.getOutputColumnPattern());

        exec.setMessage("Executing Spark multi query job...");
        final SparkMultiQueryJobOutput jobOutput = SparkContextUtil
            .<SparkMultiQueryJobInput, SparkMultiQueryJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Resolves {@code $$varName} tokens in a SQL expression by substituting the current flow
     * variable values at execution time.
     * <ul>
     *   <li>STRING variables are substituted as single-quoted SQL literals (single quotes inside
     *       the value are escaped by doubling: {@code '} → {@code ''}).</li>
     *   <li>INTEGER and DOUBLE variables are substituted as unquoted numeric literals.</li>
     *   <li>Other variable types are treated as STRING (quoted).</li>
     * </ul>
     *
     * @param sql the SQL expression template (may contain {@code $$varName} tokens)
     * @return the SQL with all {@code $$varName} tokens replaced by their current values
     * @throws InvalidSettingsException if a referenced variable is not found
     */
    @SuppressWarnings("deprecation")
    private String resolveFlowVariables(final String sql) throws InvalidSettingsException {
        if (!sql.contains("$$")) {
            return sql;
        }
        final Map<String, FlowVariable> flowVars = getAvailableFlowVariables();
        final Matcher m = FLOW_VAR_PATTERN.matcher(sql);
        final StringBuffer sb = new StringBuffer();
        while (m.find()) {
            final String varName = m.group(1);
            final FlowVariable var = flowVars.get(varName);
            if (var == null) {
                throw new InvalidSettingsException(
                    "Flow variable '$$" + varName + "' referenced in the SQL expression was not found. "
                    + "Available variables: " + String.join(", ", flowVars.keySet()));
            }
            final String replacement;
            switch (var.getType()) {
                case INTEGER:
                    replacement = String.valueOf(var.getIntValue());
                    break;
                case DOUBLE:
                    replacement = String.valueOf(var.getDoubleValue());
                    break;
                default: // STRING and any other types — quote as SQL string literal
                    replacement = "'" + var.getStringValue().replace("'", "''") + "'";
                    break;
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }
}
