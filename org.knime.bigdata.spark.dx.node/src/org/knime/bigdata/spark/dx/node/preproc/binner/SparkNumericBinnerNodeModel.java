package org.knime.bigdata.spark.dx.node.preproc.binner;

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
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Numeric Binner node. Bins numeric column values
 * into String categories using Spark SQL CASE WHEN expressions.
 */
public class SparkNumericBinnerNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkNumericBinnerNodeModel.class.getCanonicalName();

    private final SparkNumericBinnerSettings m_settings = new SparkNumericBinnerSettings();

    /** Constructor. */
    public SparkNumericBinnerNodeModel() {
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
        if (columns == null || columns.isEmpty()) {
            throw new InvalidSettingsException("No columns selected for binning.");
        }

        for (final String col : columns) {
            final int idx = tableSpec.findColumnIndex(col);
            if (idx == -1) {
                throw new InvalidSettingsException(
                    "Column '" + col + "' not found in the input table.");
            }
            final DataColumnSpec colSpec = tableSpec.getColumnSpec(idx);
            if (!colSpec.getType().isCompatible(DoubleValue.class)) {
                throw new InvalidSettingsException(
                    "Column '" + col + "' is not numeric (type: " + colSpec.getType().getName()
                    + "). Only numeric columns can be binned.");
            }
        }

        // Validate custom bins
        if (SparkNumericBinnerSettings.MODE_CUSTOM.equals(m_settings.getBinningMode())) {
            validateCustomBins();
        }

        // Validate number of bins for auto modes
        if (!SparkNumericBinnerSettings.MODE_CUSTOM.equals(m_settings.getBinningMode())) {
            if (m_settings.getNumberOfBins() < 1) {
                throw new InvalidSettingsException("Number of bins must be at least 1.");
            }
        }

        // Validate suffix for append mode
        if (!m_settings.isReplace()) {
            final String suffix = m_settings.getSuffix();
            if (suffix == null || suffix.isEmpty()) {
                throw new InvalidSettingsException("Suffix must not be empty in append mode.");
            }
            // Check for column name conflicts
            for (final String col : columns) {
                final String newName = col + suffix;
                if (tableSpec.containsName(newName)) {
                    setWarningMessage("Output column '" + newName + "' already exists and will be replaced.");
                }
            }
        }

        // Build output spec
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    /**
     * Validates custom bin definitions.
     */
    private void validateCustomBins() throws InvalidSettingsException {
        final String[] binNames = m_settings.getBinNames();
        final String[] leftBounds = m_settings.getBinLeftBounds();
        final String[] rightBounds = m_settings.getBinRightBounds();

        if (binNames == null || binNames.length == 0) {
            throw new InvalidSettingsException("No bin definitions found. Add at least one bin.");
        }

        final Set<String> usedNames = new HashSet<>();
        for (int i = 0; i < binNames.length; i++) {
            // Check bin name
            if (binNames[i] == null || binNames[i].trim().isEmpty()) {
                throw new InvalidSettingsException("Bin " + (i + 1) + " has an empty name.");
            }
            if (!usedNames.add(binNames[i])) {
                setWarningMessage(
                    "Duplicate bin name '" + binNames[i] + "' in bin " + (i + 1)
                    + ". The first matching bin will be used.");
            }

            // Check bounds
            if (i < leftBounds.length && i < rightBounds.length) {
                try {
                    final double left = Double.parseDouble(leftBounds[i]);
                    final double right = Double.parseDouble(rightBounds[i]);
                    if (left > right) {
                        throw new InvalidSettingsException(
                            "Bin " + (i + 1) + " ('" + binNames[i]
                            + "'): left boundary (" + left + ") > right boundary (" + right + ").");
                    }
                } catch (final NumberFormatException e) {
                    throw new InvalidSettingsException(
                        "Bin " + (i + 1) + " has invalid boundary values.");
                }
            }
        }

        // Check for overlapping bins (warning only, handled by setWarningMessage)
        checkOverlappingBins(binNames, leftBounds, rightBounds,
            m_settings.getBinLeftInclusive(), m_settings.getBinRightInclusive());
    }

    /**
     * Checks for overlapping bin ranges and sets a warning if found.
     */
    private void checkOverlappingBins(final String[] binNames,
            final String[] leftBounds, final String[] rightBounds,
            final String[] leftInclusive, final String[] rightInclusive) {
        for (int i = 0; i < binNames.length; i++) {
            for (int j = i + 1; j < binNames.length; j++) {
                try {
                    final double leftI = Double.parseDouble(leftBounds[i]);
                    final double rightI = Double.parseDouble(rightBounds[i]);
                    final double leftJ = Double.parseDouble(leftBounds[j]);
                    final double rightJ = Double.parseDouble(rightBounds[j]);
                    final boolean rInclI = i < rightInclusive.length
                        && Boolean.parseBoolean(rightInclusive[i]);
                    final boolean lInclJ = j < leftInclusive.length
                        && Boolean.parseBoolean(leftInclusive[j]);
                    final boolean rInclJ = j < rightInclusive.length
                        && Boolean.parseBoolean(rightInclusive[j]);
                    final boolean lInclI = i < leftInclusive.length
                        && Boolean.parseBoolean(leftInclusive[i]);

                    // Check if bin I's right overlaps with bin J's left
                    final boolean iRightOverlapsJLeft;
                    if (rightI == leftJ) {
                        iRightOverlapsJLeft = rInclI && lInclJ;
                    } else {
                        iRightOverlapsJLeft = rightI > leftJ;
                    }

                    // Check if bin J's right overlaps with bin I's left
                    final boolean jRightOverlapsILeft;
                    if (rightJ == leftI) {
                        jRightOverlapsILeft = rInclJ && lInclI;
                    } else {
                        jRightOverlapsILeft = rightJ > leftI;
                    }

                    if (iRightOverlapsJLeft && jRightOverlapsILeft) {
                        setWarningMessage("Bins '" + binNames[i] + "' and '" + binNames[j]
                            + "' have overlapping ranges. The first matching bin will be used.");
                        return;
                    }
                } catch (final NumberFormatException e) {
                    // Skip invalid bounds
                }
            }
        }
    }

    /**
     * Creates the output table spec. Binned columns are always StringType.
     * For replace mode, the original column type is changed to String.
     * For append mode, new String columns are added at the end (matching Spark behavior).
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final List<String> columns = m_settings.getColumns();
        final boolean replace = m_settings.isReplace();
        final String suffix = m_settings.getSuffix();

        final List<DataColumnSpec> outputCols = new ArrayList<>();
        final Set<String> columnsToReplace = new HashSet<>(columns);

        // Add all original columns (replacing type for replace mode)
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
            if (replace && columnsToReplace.contains(colSpec.getName())) {
                outputCols.add(new DataColumnSpecCreator(colSpec.getName(), StringCell.TYPE).createSpec());
            } else {
                outputCols.add(colSpec);
            }
        }

        // For append mode, add binned columns at the end (matching Spark's withColumn behavior)
        if (!replace) {
            for (final String col : columns) {
                outputCols.add(new DataColumnSpecCreator(
                    col + suffix, StringCell.TYPE).createSpec());
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

        final SparkNumericBinnerJobInput jobInput = new SparkNumericBinnerJobInput(
            inputObject, outputObject,
            columns.toArray(new String[0]),
            m_settings.getBinningMode(),
            m_settings.getNumberOfBins(),
            m_settings.getBinNaming(),
            m_settings.isReplace(),
            m_settings.getSuffix(),
            m_settings.getBinNames(),
            m_settings.getBinLeftBounds(),
            m_settings.getBinLeftInclusive(),
            m_settings.getBinRightBounds(),
            m_settings.getBinRightInclusive());

        exec.setMessage("Executing Spark numeric binner job...");
        final SparkNumericBinnerJobOutput jobOutput = SparkContextUtil
            .<SparkNumericBinnerJobInput, SparkNumericBinnerJobOutput>getJobRunFactory(contextID, JOB_ID)
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
