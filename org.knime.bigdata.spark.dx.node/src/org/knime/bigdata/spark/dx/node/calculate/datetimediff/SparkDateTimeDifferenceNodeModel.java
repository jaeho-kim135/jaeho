package org.knime.bigdata.spark.dx.node.calculate.datetimediff;

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
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Date&Time Difference node.
 * Calculates the difference between two date/time values using Spark SQL functions.
 */
public class SparkDateTimeDifferenceNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkDateTimeDifferenceNodeModel.class.getCanonicalName();

    private final SparkDateTimeDifferenceSettings m_settings = new SparkDateTimeDifferenceSettings();

    /** Constructor. */
    public SparkDateTimeDifferenceNodeModel() {
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

        // Validate settings
        validateConfiguration(tableSpec);

        // Build output spec: input columns + new difference column with type based on granularity
        final DataType outputType = getOutputType(m_settings.getGranularity());
        final DataColumnSpec newCol = new DataColumnSpecCreator(
            m_settings.getOutputColName(), outputType).createSpec();
        final DataColumnSpec[] cols = new DataColumnSpec[tableSpec.getNumColumns() + 1];
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            cols[i] = tableSpec.getColumnSpec(i);
        }
        cols[tableSpec.getNumColumns()] = newCol;
        final DataTableSpec outputSpec = new DataTableSpec(cols);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    /**
     * Validates the current configuration against the input table spec.
     */
    private void validateConfiguration(final DataTableSpec tableSpec) throws InvalidSettingsException {
        // Validate first column
        final String firstCol = m_settings.getFirstColumn();
        if (firstCol == null || firstCol.trim().isEmpty()) {
            throw new InvalidSettingsException("First date/time column must not be empty.");
        }
        if (tableSpec.findColumnIndex(firstCol) == -1) {
            throw new InvalidSettingsException(
                "First column '" + firstCol + "' does not exist in the input table.");
        }

        // Validate second value based on mode
        final String secondMode = m_settings.getSecondMode();
        if ("COLUMN".equals(secondMode)) {
            final String secondCol = m_settings.getSecondColumn();
            if (secondCol == null || secondCol.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Second date/time column must not be empty when mode is 'Second column'.");
            }
            if (tableSpec.findColumnIndex(secondCol) == -1) {
                throw new InvalidSettingsException(
                    "Second column '" + secondCol + "' does not exist in the input table.");
            }
        } else if ("FIXED".equals(secondMode)) {
            final String fixedVal = m_settings.getFixedDateTime();
            if (fixedVal == null || fixedVal.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Fixed date/time value must not be empty when mode is 'Fixed date/time'.");
            }
            // Basic format validation: must match yyyy-MM-dd or yyyy-MM-dd HH:mm:ss
            final String trimmed = fixedVal.trim();
            if (!trimmed.matches("\\d{4}-\\d{2}-\\d{2}(\\s+\\d{2}:\\d{2}:\\d{2})?")) {
                throw new InvalidSettingsException(
                    "Invalid date/time format: '" + trimmed + "'. "
                    + "Expected format: 'yyyy-MM-dd' or 'yyyy-MM-dd HH:mm:ss'.");
            }
        }
        // CURRENT mode needs no additional validation

        // Validate output column name
        final String outputColName = m_settings.getOutputColName();
        if (outputColName == null || outputColName.trim().isEmpty()) {
            throw new InvalidSettingsException("Output column name must not be empty.");
        }

        // Check for column name conflict
        if (tableSpec.findColumnIndex(outputColName) != -1) {
            throw new InvalidSettingsException(
                "Output column name '" + outputColName + "' already exists in the input table. "
                + "Please choose a different name.");
        }
    }

    /**
     * Returns the KNIME data type for the output column based on granularity.
     */
    private static DataType getOutputType(final String granularity) {
        switch (granularity) {
            case "YEAR":
            case "MONTH":
            case "WEEK":
            case "DAY":
                return IntCell.TYPE;
            case "HOUR":
            case "MINUTE":
                return DoubleCell.TYPE;
            case "SECOND":
            case "MILLISECOND":
            case "MICROSECOND":
            default:
                return LongCell.TYPE;
        }
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkDateTimeDifferenceJobInput jobInput = new SparkDateTimeDifferenceJobInput(
            inputObject, outputObject,
            m_settings.getFirstColumn(),
            m_settings.getSecondMode(),
            m_settings.getSecondColumn(),
            m_settings.getFixedDateTime(),
            m_settings.getDirection(),
            m_settings.getGranularity(),
            m_settings.getOutputColName());

        exec.setMessage("Executing Spark Date&Time Difference job...");
        final SparkDateTimeDifferenceJobOutput jobOutput = SparkContextUtil
            .<SparkDateTimeDifferenceJobInput, SparkDateTimeDifferenceJobOutput>getJobRunFactory(contextID, JOB_ID)
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
