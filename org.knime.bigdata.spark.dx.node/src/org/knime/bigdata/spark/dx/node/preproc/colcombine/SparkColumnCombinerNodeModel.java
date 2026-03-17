package org.knime.bigdata.spark.dx.node.preproc.colcombine;

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
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Column Combiner node. Combines multiple columns
 * into a single string column using Spark's CONCAT_WS function.
 */
public class SparkColumnCombinerNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkColumnCombinerNodeModel.class.getCanonicalName();

    private final SparkColumnCombinerSettings m_settings = new SparkColumnCombinerSettings();

    /** Constructor. */
    public SparkColumnCombinerNodeModel() {
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

        // Validate columns
        final List<String> columns = m_settings.getColumns();
        if (columns == null || columns.size() < 2) {
            throw new InvalidSettingsException(
                "At least 2 columns must be selected for combining. Currently selected: "
                + (columns == null ? 0 : columns.size()));
        }

        for (String col : columns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Column '" + col + "' not found in input table.");
            }
        }

        // Validate output column name
        final String outputColName = m_settings.getOutputColName();
        if (outputColName == null || outputColName.trim().isEmpty()) {
            throw new InvalidSettingsException("Output column name must not be empty.");
        }

        // Validate output column name does not conflict with existing columns.
        // When removeInputCols=true, only selected columns are removed, so conflicts
        // with non-selected columns must always be caught.
        // When removeInputCols=false, ALL existing columns remain, so any match is a conflict.
        final Set<String> selectedSet = new HashSet<>(columns);
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final String existingCol = tableSpec.getColumnSpec(i).getName();
            if (existingCol.equals(outputColName.trim())
                && (!m_settings.removeInputCols() || !selectedSet.contains(existingCol))) {
                throw new InvalidSettingsException(
                    "Output column name '" + outputColName.trim()
                    + "' conflicts with an existing column. "
                    + "Please choose a different name.");
            }
        }

        // Validate quoting settings
        final String quoteMode = m_settings.getQuoteMode();
        if (!SparkColumnCombinerSettings.QUOTE_NONE.equals(quoteMode)) {
            // QUOTE and REPLACE_IN_CELL modes require a non-empty delimiter
            if (m_settings.getDelimiter() == null || m_settings.getDelimiter().isEmpty()) {
                throw new InvalidSettingsException(
                    "Delimiter must not be empty when quoting or replacement mode is enabled.");
            }
        }
        if (SparkColumnCombinerSettings.QUOTE_QUOTE.equals(quoteMode)) {
            final String quoteChar = m_settings.getQuoteChar();
            if (quoteChar == null || quoteChar.isEmpty()) {
                throw new InvalidSettingsException("Quote character must not be empty when quote mode is enabled.");
            }
            if (quoteChar.equals(m_settings.getDelimiter())) {
                throw new InvalidSettingsException(
                    "Quote character must not be the same as the delimiter.");
            }
        }

        // Build output spec
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final List<DataColumnSpec> outputCols = new ArrayList<>();
        final List<String> selectedColumns = m_settings.getColumns();
        final Set<String> selectedSet = new HashSet<>(selectedColumns);

        // Add all input columns, optionally removing selected ones
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
            if (m_settings.removeInputCols() && selectedSet.contains(colSpec.getName())) {
                continue;
            }
            outputCols.add(colSpec);
        }

        // Add the combined output column (String type)
        outputCols.add(new DataColumnSpecCreator(m_settings.getOutputColName().trim(), StringCell.TYPE).createSpec());

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> columns = m_settings.getColumns();

        final SparkColumnCombinerJobInput jobInput = new SparkColumnCombinerJobInput(
            inputObject,
            outputObject,
            columns.toArray(new String[0]),
            m_settings.getDelimiter(),
            m_settings.getOutputColName().trim(),
            m_settings.removeInputCols(),
            m_settings.getHandleMissing(),
            m_settings.getQuoteMode(),
            m_settings.getQuoteChar(),
            m_settings.getReplacementDelimiter());

        exec.setMessage("Executing Spark column combiner job...");
        final SparkColumnCombinerJobOutput jobOutput = SparkContextUtil
            .<SparkColumnCombinerJobInput, SparkColumnCombinerJobOutput>getJobRunFactory(contextID, JOB_ID)
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
