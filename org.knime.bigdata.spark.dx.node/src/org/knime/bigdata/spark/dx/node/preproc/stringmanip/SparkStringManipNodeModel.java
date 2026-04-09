package org.knime.bigdata.spark.dx.node.preproc.stringmanip;

import java.util.Collections;
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
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContext;

/**
 * Node model for the Spark String Manipulation node.
 * Applies a single Spark SQL string expression to produce a new or replaced column.
 */
public class SparkStringManipNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkStringManipNodeModel.class.getCanonicalName();

    /** Pattern for flow variable placeholders: $${varName} */
    private static final Pattern FLOW_VAR_PATTERN = Pattern.compile("\\$\\$\\{([^}]+)\\}");

    private final SparkStringManipSettings m_settings = new SparkStringManipSettings();

    /** Constructor. */
    protected SparkStringManipNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame available.");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        validateSettings(tableSpec);

        // Output spec is null because expression result type is determined at runtime
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        // Resolve $${varName} flow variable placeholders
        final String resolvedExpression = resolveFlowVariables(m_settings.getExpression());

        final SparkStringManipJobInput jobInput = new SparkStringManipJobInput(
            inputObject, outputObject,
            resolvedExpression,
            m_settings.getAppendOrReplace(),
            m_settings.getOutputColName(),
            m_settings.getReplaceColumn());

        exec.setMessage("Executing Spark string manipulation job...");
        final SparkStringManipJobOutput jobOutput = SparkContextUtil
            .<SparkStringManipJobInput, SparkStringManipJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Validates settings against the input table spec.
     */
    private void validateSettings(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final String expression = m_settings.getExpression();
        if (expression == null || expression.trim().isEmpty()) {
            throw new InvalidSettingsException(
                "Expression is empty. Enter a Spark SQL string expression.");
        }

        if (m_settings.isReplace()) {
            final String replaceCol = m_settings.getReplaceColumn();
            if (replaceCol == null || replaceCol.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Replace column is not selected. Select a column to replace.");
            }
            if (tableSpec.findColumnIndex(replaceCol) == -1) {
                throw new InvalidSettingsException(
                    "Replace column '" + replaceCol + "' does not exist in the input table.");
            }
        } else {
            final String outputColName = m_settings.getOutputColName();
            if (outputColName == null || outputColName.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Output column name is empty. Enter a name for the new column.");
            }
            if (tableSpec.findColumnIndex(outputColName) != -1) {
                throw new InvalidSettingsException(
                    "Output column '" + outputColName + "' already exists in the input table. "
                    + "Use REPLACE mode or choose a different column name.");
            }
        }
    }

    /**
     * Resolves {@code $${varName}} placeholders in the expression with actual flow variable values.
     * String variables are SQL-quoted, numeric types are inserted as literals.
     */
    @SuppressWarnings("deprecation")
    private String resolveFlowVariables(final String expression) {
        Map<String, FlowVariable> flowVars = Collections.emptyMap();
        try {
            final NodeContainer nc = NodeContext.getContext().getNodeContainer();
            if (nc instanceof NativeNodeContainer) {
                final org.knime.core.node.workflow.FlowObjectStack stack = ((NativeNodeContainer) nc).getFlowObjectStack();
                if (stack != null) {
                    flowVars = stack.getAvailableFlowVariables(FlowVariable.Type.values());
                }
            }
        } catch (final Exception e) {
            // Fall through with empty map
        }

        final Matcher matcher = FLOW_VAR_PATTERN.matcher(expression);
        final StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            final String varName = matcher.group(1);
            final FlowVariable fv = flowVars.get(varName);
            if (fv != null) {
                final String replacement;
                switch (fv.getType()) {
                    case INTEGER:
                        replacement = String.valueOf(fv.getIntValue());
                        break;
                    case DOUBLE:
                        replacement = String.valueOf(fv.getDoubleValue());
                        break;
                    case STRING:
                        replacement = "'" + fv.getStringValue().replace("'", "''") + "'";
                        break;
                    default:
                        replacement = "'" + fv.getValueAsString().replace("'", "''") + "'";
                        break;
                }
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
        }
        matcher.appendTail(sb);
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
