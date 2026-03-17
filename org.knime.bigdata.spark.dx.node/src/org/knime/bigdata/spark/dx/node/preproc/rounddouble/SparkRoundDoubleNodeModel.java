package org.knime.bigdata.spark.dx.node.preproc.rounddouble;

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
import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Number Rounder node.
 * Rounds numeric columns using configurable rounding methods.
 */
public class SparkRoundDoubleNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkRoundDoubleNodeModel.class.getCanonicalName();

    private final SparkRoundDoubleSettings m_settings = new SparkRoundDoubleSettings();

    /** Constructor. */
    public SparkRoundDoubleNodeModel() {
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

        // Validate selected columns
        final List<String> columns = m_settings.getColumns();
        if (columns == null || columns.isEmpty()) {
            setWarningMessage("No columns selected for rounding. Output will be identical to input.");
        } else {
            for (final String col : columns) {
                if (tableSpec.findColumnIndex(col) == -1) {
                    throw new InvalidSettingsException(
                        "Column '" + col + "' not found in input table.");
                }
            }
        }

        // Validate precision
        final String numberMode = m_settings.getNumberMode();
        final int precision = m_settings.getPrecision();
        if (!"INTEGER".equals(numberMode) && precision < 0) {
            throw new InvalidSettingsException(
                "Precision must not be negative.");
        }
        if ("SIGNIFICANT_DIGITS".equals(numberMode) && precision < 1) {
            throw new InvalidSettingsException(
                "Precision must be at least 1 for Significant digits mode.");
        }
        if ("INTEGER".equals(numberMode)) {
            setWarningMessage("In INTEGER mode, very large values may exceed Long range and produce null results.");
        }

        // Validate suffix in APPEND mode
        if (!m_settings.isReplace()) {
            final String suffix = m_settings.getSuffix();
            if (suffix == null || suffix.isEmpty()) {
                throw new InvalidSettingsException(
                    "Suffix must not be empty in Append mode.");
            }
            // Check for column name conflicts
            if (columns != null) {
                for (final String col : columns) {
                    final String newName = col + suffix;
                    if (tableSpec.findColumnIndex(newName) != -1) {
                        throw new InvalidSettingsException(
                            "Output column '" + newName + "' already exists in the input table. "
                            + "Please choose a different suffix.");
                    }
                }
            }
        }

        // Build output spec: deterministic based on number mode
        // DECIMALS/SIGNIFICANT_DIGITS → DoubleType, INTEGER → LongType
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    /**
     * Creates the output table spec based on the rounding configuration.
     * For REPLACE mode, rounded columns change type. For APPEND mode, new columns are added.
     * DECIMALS/SIGNIFICANT_DIGITS produce DoubleType, INTEGER produces LongType.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final List<String> columns = m_settings.getColumns();
        if (columns == null || columns.isEmpty()) {
            return inputSpec;
        }

        final boolean replace = m_settings.isReplace();
        final String numberMode = m_settings.getNumberMode();
        final String suffix = m_settings.getSuffix();

        // INTEGER mode → LongType, otherwise DoubleType
        final DataType outputType = "INTEGER".equals(numberMode) ? LongCell.TYPE : DoubleCell.TYPE;

        final List<DataColumnSpec> outputCols = new ArrayList<>();
        final Set<String> columnsToRound = new HashSet<>(columns);

        // Add all original columns (replacing type for replace mode)
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
            if (replace && columnsToRound.contains(colSpec.getName())) {
                outputCols.add(new DataColumnSpecCreator(colSpec.getName(), outputType).createSpec());
            } else {
                outputCols.add(colSpec);
            }
        }

        // For append mode, add rounded columns at the end
        if (!replace) {
            for (final String col : columns) {
                outputCols.add(new DataColumnSpecCreator(col + suffix, outputType).createSpec());
            }
        }

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> columns = m_settings.getColumns();
        if (columns == null || columns.isEmpty()) {
            // No columns selected: pass through input as output
            setWarningMessage("No columns selected for rounding. Output is identical to input.");
            return new PortObject[]{sparkPort};
        }

        final SparkRoundDoubleJobInput jobInput = new SparkRoundDoubleJobInput(
            inputObject, outputObject,
            columns.toArray(new String[0]),
            m_settings.getNumberMode(),
            m_settings.getPrecision(),
            m_settings.getRoundingStandard(),
            m_settings.getRoundingAdvanced(),
            m_settings.isReplace(),
            m_settings.getSuffix());

        exec.setMessage("Executing Spark number rounding job...");
        final SparkRoundDoubleJobOutput jobOutput = SparkContextUtil
            .<SparkRoundDoubleJobInput, SparkRoundDoubleJobOutput>getJobRunFactory(contextID, JOB_ID)
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
