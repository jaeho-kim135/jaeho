package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionJobInput;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionJobOutput;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionNodeModel;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.WorkflowManager;

/**
 * RPC service for the Spark Rule Engine WebUI dialog.
 * Provides rule validation and preview by running Spark jobs.
 */
@SuppressWarnings("restriction")
public final class SparkRuleEngineRpcService {

    /** Old format: $${SvarName}$$, $${IvarName}$$, $${DvarName}$$ (with type prefix + trailing $$) */
    private static final Pattern FLOW_VAR_OLD = Pattern.compile("\\$\\$\\{([SID])([^}]+)\\}\\$\\$");

    /** New format: $${varName} (auto-detect type from available flow variables) */
    private static final Pattern FLOW_VAR_NEW = Pattern.compile("\\$\\$\\{([^}]+)\\}");

    private final NodeContext m_nodeContext;

    SparkRuleEngineRpcService(final NodeContext nodeContext) {
        m_nodeContext = nodeContext;
    }

    /**
     * Validate and preview rules on the Spark cluster.
     *
     * @param rules the multiline rule text
     * @param defaultValue the default value for non-matching rows
     * @param defaultIsMissing true if default should be null
     * @param appendOrReplace "APPEND" or "REPLACE"
     * @param outputColumnName output column name (APPEND mode)
     * @param replaceColumn column to replace (REPLACE mode)
     * @return map with keys: "success" (Boolean), "preview" or "error" (String)
     */
    public Map<String, Object> evaluateRules(final String rules, final String defaultValue,
            final boolean defaultIsMissing, final String appendOrReplace,
            final String outputColumnName, final String replaceColumn) {

        final Map<String, Object> result = new LinkedHashMap<>();

        if (rules == null || rules.trim().isEmpty()) {
            result.put("success", false);
            result.put("error", "No rules to evaluate. Enter at least one rule.");
            return result;
        }

        try {
            NodeContext.pushContext(m_nodeContext);

            final SparkDataPortObject sparkPort = findSparkInputPort();
            if (sparkPort == null) {
                result.put("success", false);
                result.put("error", "Execute the upstream node first to enable evaluation.");
                return result;
            }

            final SparkContextID contextID = sparkPort.getContextID();
            final String inputObjectId = sparkPort.getData().getID();

            // Resolve flow variables
            final String resolvedRules = resolveFlowVariables(rules);
            final String[] ruleLines = resolvedRules.split("\\r?\\n");

            final boolean isReplace = "REPLACE".equals(appendOrReplace);
            final String outputColumn = isReplace ? replaceColumn : outputColumnName;

            final SparkRuleEngineJobInput jobInput = new SparkRuleEngineJobInput(
                inputObjectId,
                ruleLines, defaultValue, defaultIsMissing,
                isReplace, outputColumn);

            final SparkRuleEngineJobOutput output = SparkContextUtil
                .<SparkRuleEngineJobInput, SparkRuleEngineJobOutput>getJobRunFactory(
                    contextID, SparkRuleEngineNodeModel.JOB_ID)
                .createRun(jobInput)
                .run(contextID, new ExecutionMonitor());

            final String preview = output.getPreviewData();
            result.put("success", true);
            result.put("preview", preview != null ? preview : "");

        } catch (final Exception e) {
            result.put("success", false);
            result.put("error", extractErrorMessage(e));
        } finally {
            NodeContext.removeLastContext();
        }
        return result;
    }

    /**
     * Preview the input table data without applying any rules.
     * Reuses the Expression job with empty expressions to call showString().
     */
    public Map<String, Object> previewInputTable() {
        final Map<String, Object> result = new LinkedHashMap<>();
        try {
            NodeContext.pushContext(m_nodeContext);

            final SparkDataPortObject sparkPort = findSparkInputPort();
            if (sparkPort == null) {
                result.put("success", false);
                result.put("error", "Execute the upstream node first to view input data.");
                return result;
            }

            final SparkContextID contextID = sparkPort.getContextID();
            final String dataFrameID = sparkPort.getData().getID();

            // Use Expression job with empty arrays to get input preview
            final SparkExpressionJobInput jobInput = new SparkExpressionJobInput(
                dataFrameID, new String[0], new String[0], new String[0]);

            final SparkExpressionJobOutput output = SparkContextUtil
                .<SparkExpressionJobInput, SparkExpressionJobOutput>getJobRunFactory(
                    contextID, SparkExpressionNodeModel.JOB_ID)
                .createRun(jobInput)
                .run(contextID, new ExecutionMonitor());

            final String preview = output.getPreviewData();
            result.put("success", true);
            result.put("preview", preview != null ? preview : "");

        } catch (final Exception e) {
            result.put("success", false);
            result.put("error", extractErrorMessage(e));
        } finally {
            NodeContext.removeLastContext();
        }
        return result;
    }

    /**
     * Resolves flow variable placeholders in rule text.
     * Supports both old format ($${SvarName}$$) and new format ($${varName}).
     */
    @SuppressWarnings("deprecation")
    private String resolveFlowVariables(String rulesText) {
        final NodeContainer nc = m_nodeContext.getNodeContainer();
        Map<String, FlowVariable> flowVars = Collections.emptyMap();
        if (nc instanceof NativeNodeContainer) {
            final FlowObjectStack stack = ((NativeNodeContainer) nc).getFlowObjectStack();
            if (stack != null) {
                flowVars = stack.getAvailableFlowVariables(FlowVariable.Type.values());
            }
        }

        // Pass 1: Resolve old format $${SvarName}$$ (with explicit type prefix)
        Matcher m1 = FLOW_VAR_OLD.matcher(rulesText);
        StringBuilder sb1 = new StringBuilder();
        while (m1.find()) {
            final String typePrefix = m1.group(1);
            final String varName = m1.group(2);
            final FlowVariable fv = flowVars.get(varName);
            if (fv != null) {
                m1.appendReplacement(sb1, Matcher.quoteReplacement(resolveByPrefix(fv, typePrefix)));
            }
        }
        m1.appendTail(sb1);
        rulesText = sb1.toString();

        // Pass 2: Resolve new format $${varName} (auto-detect type)
        Matcher m2 = FLOW_VAR_NEW.matcher(rulesText);
        StringBuilder sb2 = new StringBuilder();
        while (m2.find()) {
            final String varName = m2.group(1);
            final FlowVariable fv = flowVars.get(varName);
            if (fv != null) {
                m2.appendReplacement(sb2, Matcher.quoteReplacement(resolveAutoType(fv)));
            }
        }
        m2.appendTail(sb2);
        return sb2.toString();
    }

    @SuppressWarnings("deprecation")
    private static String resolveByPrefix(final FlowVariable fv, final String typePrefix) {
        switch (typePrefix) {
            case "I": return String.valueOf(fv.getIntValue());
            case "D": return String.valueOf(fv.getDoubleValue());
            case "S":
            default:  return "'" + fv.getStringValue().replace("'", "''") + "'";
        }
    }

    @SuppressWarnings("deprecation")
    private static String resolveAutoType(final FlowVariable fv) {
        switch (fv.getType()) {
            case INTEGER: return String.valueOf(fv.getIntValue());
            case DOUBLE:  return String.valueOf(fv.getDoubleValue());
            case STRING:  return "'" + fv.getStringValue().replace("'", "''") + "'";
            default:      return "'" + fv.getValueAsString().replace("'", "''") + "'";
        }
    }

    private SparkDataPortObject findSparkInputPort() {
        try {
            final NodeContainer thisNC = m_nodeContext.getNodeContainer();
            if (!(thisNC instanceof NativeNodeContainer)) {
                return null;
            }
            final NativeNodeContainer nc = (NativeNodeContainer) thisNC;
            final WorkflowManager wfm = nc.getParent();
            final int numPorts = nc.getNrInPorts();

            for (int i = 1; i < numPorts; i++) {
                final ConnectionContainer cc = wfm.getIncomingConnectionFor(nc.getID(), i);
                if (cc == null) {
                    continue;
                }
                final NodeContainer sourceNC = wfm.getNodeContainer(cc.getSource());
                final PortObject portObject = sourceNC.getOutPort(cc.getSourcePort()).getPortObject();
                if (portObject instanceof SparkDataPortObject) {
                    return (SparkDataPortObject) portObject;
                }
            }
        } catch (final Exception e) {
            // Return null on error
        }
        return null;
    }

    private static String extractErrorMessage(final Exception e) {
        Throwable current = e;
        String firstMsg = null;
        while (current != null) {
            final String msg = current.getMessage();
            if (msg != null && !msg.trim().isEmpty()) {
                if (firstMsg == null) {
                    firstMsg = msg;
                }
                if (msg.contains("cannot resolve") || msg.contains("Column")
                        || msg.contains("AnalysisException") || msg.contains("rule")) {
                    return msg;
                }
            }
            current = current.getCause();
        }
        return firstMsg != null ? firstMsg : "Unknown error during rule evaluation.";
    }
}
