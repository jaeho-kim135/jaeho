package org.knime.bigdata.spark.dx.node.preproc.cellsplit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
 * Node model for the Spark Cell Splitter node.
 * Splits a string column into multiple columns using a delimiter.
 */
public class SparkCellSplitterNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkCellSplitterNodeModel.class.getCanonicalName();

    private final SparkCellSplitterSettings m_settings = new SparkCellSplitterSettings();

    /** Constructor. */
    public SparkCellSplitterNodeModel() {
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

        // Validate column selection
        final String column = m_settings.getColumn();
        if (column == null || column.trim().isEmpty()) {
            throw new InvalidSettingsException("No column selected. Please select a string column to split.");
        }
        final int colIdx = tableSpec.findColumnIndex(column);
        if (colIdx == -1) {
            throw new InvalidSettingsException(
                "Selected column '" + column + "' not found in the input table.");
        }
        final DataColumnSpec colSpec = tableSpec.getColumnSpec(colIdx);
        if (!colSpec.getType().isCompatible(StringValue.class)) {
            throw new InvalidSettingsException(
                "Selected column '" + column + "' is not a string column (type: "
                + colSpec.getType().toPrettyString() + ").");
        }

        // Validate delimiter
        final String delimiter = m_settings.getDelimiter();
        if (delimiter == null || delimiter.isEmpty()) {
            throw new InvalidSettingsException("Delimiter must not be empty.");
        }

        // Validate regex pattern if regex mode enabled
        if (m_settings.isUseRegex()) {
            try {
                java.util.regex.Pattern.compile(delimiter);
            } catch (final java.util.regex.PatternSyntaxException e) {
                throw new InvalidSettingsException(
                    "Invalid regular expression delimiter: " + e.getMessage());
            }
        }

        // Validate fixed size
        if (m_settings.isFixedMode() && m_settings.getFixedSize() < 1) {
            throw new InvalidSettingsException(
                "Number of output columns must be at least 1.");
        }

        // Validate scan limit in AUTO mode
        if (!m_settings.isFixedMode() && m_settings.getScanLimit() < 1) {
            throw new InvalidSettingsException(
                "Row scan limit must be at least 1.");
        }

        // In FIXED mode, we can determine the output spec upfront
        if (m_settings.isFixedMode()) {
            final DataTableSpec outputSpec = createOutputSpec(tableSpec, m_settings.getFixedSize());
            return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
        }

        // In AUTO mode, the number of output columns is determined at runtime
        return new PortObjectSpec[]{null};
    }

    /**
     * Creates the output spec by appending N StringType columns to the input spec.
     *
     * @param inputSpec the input table spec
     * @param numCols the number of split columns to add
     * @return the output table spec
     * @throws InvalidSettingsException if output column names conflict with existing columns
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec, final int numCols)
            throws InvalidSettingsException {
        final String column = m_settings.getColumn();
        final String prefix = m_settings.getOutputPrefix().isEmpty() ? column : m_settings.getOutputPrefix();

        // Collect ALL existing column names (including source column) to prevent
        // withColumn/drop data loss when an output name matches the source column
        final Set<String> existingNames = new HashSet<>();
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            existingNames.add(inputSpec.getColumnSpec(i).getName());
        }

        // Check for conflicts with output column names
        for (int i = 1; i <= numCols; i++) {
            final String outputName = prefix + "_" + i;
            if (existingNames.contains(outputName)) {
                throw new InvalidSettingsException(
                    "Output column name '" + outputName + "' conflicts with an existing column. "
                    + "Use a different output column prefix.");
            }
        }

        // Build output spec
        final List<DataColumnSpec> outputCols = new ArrayList<>();
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
            if (m_settings.isRemoveInputCol() && colSpec.getName().equals(column)) {
                continue;
            }
            outputCols.add(colSpec);
        }
        for (int i = 1; i <= numCols; i++) {
            outputCols.add(new DataColumnSpecCreator(prefix + "_" + i, StringCell.TYPE).createSpec());
        }

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkCellSplitterJobInput jobInput = new SparkCellSplitterJobInput(
            inputObject,
            outputObject,
            m_settings.getColumn(),
            m_settings.getDelimiter(),
            m_settings.isUseRegex(),
            m_settings.getSizeMode(),
            m_settings.getFixedSize(),
            m_settings.getScanLimit(),
            m_settings.isTrim(),
            m_settings.isUseEmptyString(),
            m_settings.isRemoveInputCol(),
            m_settings.getOutputPrefix());

        exec.setMessage("Executing Spark cell splitter job...");
        final SparkCellSplitterJobOutput jobOutput = SparkContextUtil
            .<SparkCellSplitterJobInput, SparkCellSplitterJobOutput>getJobRunFactory(contextID, JOB_ID)
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
