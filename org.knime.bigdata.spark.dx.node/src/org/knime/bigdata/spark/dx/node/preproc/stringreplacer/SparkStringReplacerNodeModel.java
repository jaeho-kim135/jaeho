package org.knime.bigdata.spark.dx.node.preproc.stringreplacer;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark String Replacer node.
 * Replaces strings in a column using literal, wildcard, or regex matching.
 */
public class SparkStringReplacerNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkStringReplacerNodeModel.class.getCanonicalName();

    private final SparkStringReplacerSettings m_settings = new SparkStringReplacerSettings();

    /** Constructor. */
    public SparkStringReplacerNodeModel() {
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

        // Validate target column
        final String column = m_settings.getColumn();
        if (column == null || column.trim().isEmpty()) {
            throw new InvalidSettingsException("No target column selected. "
                + "Please select a string column to apply the replacement to.");
        }

        final int colIdx = tableSpec.findColumnIndex(column);
        if (colIdx < 0) {
            throw new InvalidSettingsException(
                "Target column '" + column + "' not found in input table.");
        }

        // Verify column is string-compatible
        final DataColumnSpec colSpec = tableSpec.getColumnSpec(colIdx);
        if (!colSpec.getType().isCompatible(StringValue.class)) {
            throw new InvalidSettingsException(
                "Target column '" + column + "' is not a string column (type: "
                + colSpec.getType().toPrettyString() + ").");
        }

        // Validate pattern
        final String pattern = m_settings.getPattern();
        if (pattern == null || pattern.isEmpty()) {
            throw new InvalidSettingsException("Search pattern must not be empty.");
        }

        // Validate regex
        if ("REGEX".equals(m_settings.getPatternType())) {
            try {
                Pattern.compile(pattern);
            } catch (final PatternSyntaxException e) {
                throw new InvalidSettingsException(
                    "Invalid regular expression: " + e.getMessage());
            }
        }

        // Validate APPEND settings
        if (m_settings.isAppend()) {
            final String newColName = m_settings.getNewColName();
            if (newColName == null || newColName.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "New column name must not be empty when output mode is 'Append new column'.");
            }

            // Check for column name conflict
            if (tableSpec.findColumnIndex(newColName) >= 0) {
                throw new InvalidSettingsException(
                    "Output column name '" + newColName + "' already exists in the input table. "
                    + "Please choose a different name.");
            }
        }

        // Build output spec (StringType is always known at configure time)
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    /**
     * Creates the output table spec. The result column is always StringType.
     * In REPLACE mode, the original column type is changed to StringType.
     * In APPEND mode, a new StringType column is appended.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final String column = m_settings.getColumn();

        if (m_settings.isReplace()) {
            // Replace mode: rebuild spec with StringType for the target column
            final DataColumnSpec[] cols = new DataColumnSpec[inputSpec.getNumColumns()];
            for (int i = 0; i < inputSpec.getNumColumns(); i++) {
                final DataColumnSpec cs = inputSpec.getColumnSpec(i);
                if (cs.getName().equals(column)) {
                    cols[i] = new DataColumnSpecCreator(cs.getName(), StringCell.TYPE).createSpec();
                } else {
                    cols[i] = cs;
                }
            }
            return new DataTableSpec(cols);
        } else {
            // Append mode: add new StringType column
            final DataColumnSpec newCol =
                new DataColumnSpecCreator(m_settings.getNewColName(), StringCell.TYPE).createSpec();
            final DataColumnSpec[] cols = new DataColumnSpec[inputSpec.getNumColumns() + 1];
            for (int i = 0; i < inputSpec.getNumColumns(); i++) {
                cols[i] = inputSpec.getColumnSpec(i);
            }
            cols[inputSpec.getNumColumns()] = newCol;
            return new DataTableSpec(cols);
        }
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkStringReplacerJobInput jobInput = new SparkStringReplacerJobInput(
            inputObject,
            outputObject,
            m_settings.getColumn(),
            m_settings.getPatternType(),
            m_settings.getPattern(),
            m_settings.getReplacement(),
            m_settings.isCaseSensitive(),
            m_settings.isEnableEscaping(),
            m_settings.getReplacementStrategy(),
            m_settings.getAppendOrReplace(),
            m_settings.getNewColName());

        exec.setMessage("Executing Spark string replacer job...");
        final SparkStringReplacerJobOutput jobOutput = SparkContextUtil
            .<SparkStringReplacerJobInput, SparkStringReplacerJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
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
