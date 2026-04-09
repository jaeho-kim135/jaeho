package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContext;

/**
 * Node model for the Spark Rule Engine node.
 * Converts IF-THEN rules to Spark SQL CASE WHEN expressions.
 */
public class SparkRuleEngineNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkRuleEngineNodeModel.class.getCanonicalName();

    /** Old format: $${SvarName}$$, $${IvarName}$$, $${DvarName}$$ (with type prefix + trailing $$) */
    private static final Pattern FLOW_VAR_OLD = Pattern.compile("\\$\\$\\{([SID])([^}]+)\\}\\$\\$");

    /** New format: $${varName} (auto-detect type from available flow variables) */
    private static final Pattern FLOW_VAR_NEW = Pattern.compile("\\$\\$\\{([^}]+)\\}");

    private final SparkRuleEngineSettings m_settings = new SparkRuleEngineSettings();

    /** Constructor. */
    public SparkRuleEngineNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame available.");
        }

        if (!m_settings.isNodeConfigured()) {
            throw new InvalidSettingsException(
                "Node has not been configured. Open the dialog and enter rules.");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        validateRules(tableSpec);

        // Output spec is null because CASE WHEN result type is determined at runtime
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final String rules = m_settings.getRules();
        final String[] ruleLines = resolveFlowVariables(rules).split("\\r?\\n");

        final String defaultValue = m_settings.getDefaultValue();
        final boolean defaultIsMissing = m_settings.isDefaultMissing();
        final boolean isReplace = m_settings.isReplace();
        final String outputColumn = m_settings.getEffectiveOutputColumn();

        final SparkRuleEngineJobInput jobInput = new SparkRuleEngineJobInput(
            inputObject, outputObject,
            ruleLines, defaultValue, defaultIsMissing,
            isReplace, outputColumn);

        exec.setMessage("Executing Spark Rule Engine job...");
        final SparkRuleEngineJobOutput jobOutput = SparkContextUtil
            .<SparkRuleEngineJobInput, SparkRuleEngineJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Validates rules against the input table spec.
     */
    private void validateRules(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final String rules = m_settings.getRules();

        if (rules == null || rules.trim().isEmpty()) {
            throw new InvalidSettingsException("No rules defined. Enter at least one rule.");
        }

        final String[] lines = rules.split("\\r?\\n");
        boolean hasValidRule = false;

        for (int i = 0; i < lines.length; i++) {
            final String line = lines[i].trim();
            if (line.isEmpty() || line.startsWith("//")) {
                continue; // skip blank lines and comments
            }

            final int arrowIdx = findArrowSeparator(line);
            if (arrowIdx < 0) {
                throw new InvalidSettingsException(
                    "Line " + (i + 1) + ": missing '=>' separator. "
                    + "Each rule must be in the format: condition => outcome");
            }

            final String condition = line.substring(0, arrowIdx).trim();
            final String outcome = line.substring(arrowIdx + 2).trim();

            if (condition.isEmpty()) {
                throw new InvalidSettingsException(
                    "Line " + (i + 1) + ": condition part is empty.");
            }
            if (outcome.isEmpty()) {
                throw new InvalidSettingsException(
                    "Line " + (i + 1) + ": outcome part is empty.");
            }

            hasValidRule = true;
        }

        if (!hasValidRule) {
            throw new InvalidSettingsException(
                "No valid rules found. Enter at least one rule (comments and blank lines are ignored).");
        }

        // Validate output column
        final String outputColumn = m_settings.getEffectiveOutputColumn();
        if (outputColumn == null || outputColumn.trim().isEmpty()) {
            throw new InvalidSettingsException("Output column name must not be empty.");
        }

        if (m_settings.isReplace()) {
            // REPLACE mode: target column must exist
            if (tableSpec.findColumnIndex(outputColumn) == -1) {
                throw new InvalidSettingsException(
                    "Replace column '" + outputColumn + "' does not exist in the input table.");
            }
        } else {
            // APPEND mode: column name must not conflict with existing columns
            final Set<String> existingColumns = new HashSet<>();
            for (int i = 0; i < tableSpec.getNumColumns(); i++) {
                existingColumns.add(tableSpec.getColumnSpec(i).getName());
            }
            if (existingColumns.contains(outputColumn)) {
                throw new InvalidSettingsException(
                    "Output column '" + outputColumn + "' already exists in the input table. "
                    + "Use Replace mode or choose a different name.");
            }
        }
    }

    /**
     * Resolves flow variable placeholders in the rules text.
     * Supports both old format ($${SvarName}$$) and new format ($${varName}).
     * String variables are SQL-quoted, numeric types are inserted as literals.
     */
    @SuppressWarnings("deprecation")
    private String resolveFlowVariables(String rulesText) throws InvalidSettingsException {
        Map<String, FlowVariable> flowVars = Collections.emptyMap();
        try {
            final NodeContainer nc = NodeContext.getContext().getNodeContainer();
            if (nc instanceof NativeNodeContainer) {
                final FlowObjectStack stack = ((NativeNodeContainer) nc).getFlowObjectStack();
                if (stack != null) {
                    flowVars = stack.getAvailableFlowVariables(FlowVariable.Type.values());
                }
            }
        } catch (final Exception e) {
            // Fall through with empty map
        }

        // Pass 1: Resolve old format $${SvarName}$$ (with explicit type prefix)
        final Matcher m1 = FLOW_VAR_OLD.matcher(rulesText);
        final StringBuilder sb1 = new StringBuilder();
        while (m1.find()) {
            final String typePrefix = m1.group(1);
            final String varName = m1.group(2);
            final FlowVariable fv = flowVars.get(varName);
            if (fv != null) {
                final String replacement;
                switch (typePrefix) {
                    case "I": replacement = String.valueOf(fv.getIntValue()); break;
                    case "D": replacement = String.valueOf(fv.getDoubleValue()); break;
                    case "S":
                    default:  replacement = "'" + fv.getStringValue().replace("'", "''") + "'"; break;
                }
                m1.appendReplacement(sb1, Matcher.quoteReplacement(replacement));
            } else {
                throw new InvalidSettingsException(
                    "Flow variable '" + varName + "' (type " + typePrefix + ") not found.");
            }
        }
        m1.appendTail(sb1);
        rulesText = sb1.toString();

        // Pass 2: Resolve new format $${varName} (auto-detect type)
        final Matcher m2 = FLOW_VAR_NEW.matcher(rulesText);
        final StringBuilder sb2 = new StringBuilder();
        while (m2.find()) {
            final String varName = m2.group(1);
            final FlowVariable fv = flowVars.get(varName);
            if (fv != null) {
                final String replacement;
                switch (fv.getType()) {
                    case INTEGER: replacement = String.valueOf(fv.getIntValue()); break;
                    case DOUBLE:  replacement = String.valueOf(fv.getDoubleValue()); break;
                    case STRING:  replacement = "'" + fv.getStringValue().replace("'", "''") + "'"; break;
                    default:      replacement = "'" + fv.getValueAsString().replace("'", "''") + "'"; break;
                }
                m2.appendReplacement(sb2, Matcher.quoteReplacement(replacement));
            } else {
                throw new InvalidSettingsException(
                    "Flow variable '" + varName + "' not found.");
            }
        }
        m2.appendTail(sb2);
        return sb2.toString();
    }

    /**
     * Finds the position of the {@code =>} rule separator that is not inside
     * a double-quoted string literal.
     *
     * @param line the rule line to search
     * @return the index of {@code =} in {@code =>}, or -1 if not found
     */
    private static int findArrowSeparator(final String line) {
        boolean inDoubleQuote = false;
        for (int i = 0; i < line.length() - 1; i++) {
            final char c = line.charAt(i);
            if (c == '"') {
                inDoubleQuote = !inDoubleQuote;
            } else if (!inDoubleQuote && c == '=' && line.charAt(i + 1) == '>') {
                return i;
            }
        }
        return -1;
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
