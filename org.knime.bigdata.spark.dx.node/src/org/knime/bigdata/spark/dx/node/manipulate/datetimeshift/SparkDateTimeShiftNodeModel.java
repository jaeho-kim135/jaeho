package org.knime.bigdata.spark.dx.node.manipulate.datetimeshift;

import java.util.List;

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

/**
 * Node model for the Spark Date&amp;Time Shift node.
 * Shifts date/time columns by a fixed or column-based value using Spark SQL functions.
 */
public class SparkDateTimeShiftNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkDateTimeShiftNodeModel.class.getCanonicalName();

    private final SparkDateTimeShiftSettings m_settings = new SparkDateTimeShiftSettings();

    /** Constructor. */
    public SparkDateTimeShiftNodeModel() {
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
        if (columns.isEmpty()) {
            throw new InvalidSettingsException("No date/time columns selected.");
        }
        for (String col : columns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException(
                    "Column '" + col + "' not found in the input table.");
            }
        }

        // Validate shift mode
        final String shiftMode = m_settings.getShiftMode();
        if ("COLUMN".equals(shiftMode)) {
            final String shiftCol = m_settings.getShiftColumn();
            if (shiftCol == null || shiftCol.isEmpty()) {
                throw new InvalidSettingsException(
                    "No shift column selected. In column mode, a column containing shift values is required.");
            }
            if (tableSpec.findColumnIndex(shiftCol) == -1) {
                throw new InvalidSettingsException(
                    "Shift column '" + shiftCol + "' not found in the input table.");
            }
        }

        // Warn about DateType + time-level granularity
        final String granularity = m_settings.getGranularity();
        if ("HOUR".equals(granularity) || "MINUTE".equals(granularity)
                || "SECOND".equals(granularity) || "MILLISECOND".equals(granularity)) {
            setWarningMessage("Applying " + granularity
                + " shift to Date columns will implicitly convert them to Timestamp. "
                + "Time information may be added where none existed.");
        }

        // Validate suffix in APPEND mode
        if (!m_settings.isReplace()) {
            final String suffix = m_settings.getSuffix();
            if (suffix == null || suffix.isEmpty()) {
                throw new InvalidSettingsException(
                    "Suffix must not be empty in Append mode.");
            }
            // Check for column name conflicts
            for (final String col : columns) {
                final String newName = col + suffix;
                if (tableSpec.containsName(newName)) {
                    setWarningMessage("Output column '" + newName
                        + "' already exists and will be replaced.");
                }
            }
        }

        // Return null spec because date type may change (e.g., DateType + HOUR -> TimestampType)
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkDateTimeShiftJobInput jobInput = new SparkDateTimeShiftJobInput(
            inputObject, outputObject,
            m_settings.getColumns().toArray(new String[0]),
            m_settings.getShiftMode(),
            m_settings.getShiftValue(),
            m_settings.getShiftColumn(),
            m_settings.getGranularity(),
            m_settings.isReplace(),
            m_settings.getSuffix());

        exec.setMessage("Executing Spark Date&Time Shift job...");
        final SparkDateTimeShiftJobOutput jobOutput = SparkContextUtil
            .<SparkDateTimeShiftJobInput, SparkDateTimeShiftJobOutput>getJobRunFactory(contextID, JOB_ID)
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
