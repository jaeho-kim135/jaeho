package org.knime.bigdata.spark.dx.node.extract.datetimefields;

import java.util.ArrayList;
import java.util.List;

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
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Extract Date&amp;Time Fields node.
 * Extracts individual date/time fields from a source column using Spark SQL functions.
 */
public class SparkExtractDateTimeFieldsNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkExtractDateTimeFieldsNodeModel.class.getCanonicalName();

    private final SparkExtractDateTimeFieldsSettings m_settings = new SparkExtractDateTimeFieldsSettings();

    /** Constructor. */
    public SparkExtractDateTimeFieldsNodeModel() {
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

        // Build output spec
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkExtractDateTimeFieldsJobInput jobInput = new SparkExtractDateTimeFieldsJobInput(
            inputObject, outputObject,
            m_settings.getColumn(),
            m_settings.isExtractYear(),
            m_settings.isExtractMonth(),
            m_settings.isExtractDay(),
            m_settings.isExtractHour(),
            m_settings.isExtractMinute(),
            m_settings.isExtractSecond(),
            m_settings.isExtractDayOfWeek(),
            m_settings.isExtractDayOfYear(),
            m_settings.isExtractWeekOfYear(),
            m_settings.isExtractQuarter(),
            m_settings.isExtractSubsecond(),
            m_settings.getSubsecondUnit(),
            m_settings.isExtractDayOfWeekName(),
            m_settings.isExtractMonthName(),
            m_settings.getLocale(),
            m_settings.getColumnPrefix());

        exec.setMessage("Executing Spark extract date/time fields job...");
        final SparkExtractDateTimeFieldsJobOutput jobOutput = SparkContextUtil
            .<SparkExtractDateTimeFieldsJobInput, SparkExtractDateTimeFieldsJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Validates the current configuration against the input table spec.
     */
    private void validateConfiguration(final DataTableSpec tableSpec) throws InvalidSettingsException {
        // Validate column is selected
        final String column = m_settings.getColumn();
        if (column == null || column.trim().isEmpty()) {
            throw new InvalidSettingsException(
                "No date/time column selected. Please select a column to extract fields from.");
        }

        // Validate column exists in input
        if (tableSpec.findColumnIndex(column) == -1) {
            throw new InvalidSettingsException(
                "Selected column '" + column + "' does not exist in the input table.");
        }

        // Validate at least one field is selected
        if (!m_settings.isAnyFieldSelected()) {
            throw new InvalidSettingsException(
                "No fields selected for extraction. Please select at least one field to extract.");
        }

        // Validate output column name conflicts
        final String prefix = m_settings.getColumnPrefix();
        final List<String> outputColNames = getExtractedColumnNames(prefix);
        for (final String name : outputColNames) {
            if (tableSpec.findColumnIndex(name) != -1) {
                throw new InvalidSettingsException(
                    "Output column '" + name + "' already exists in the input table. "
                    + "Use a column prefix to avoid name conflicts.");
            }
        }
    }

    /**
     * Creates the output DataTableSpec with extracted columns appended.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final String prefix = m_settings.getColumnPrefix();
        final List<DataColumnSpec> outputCols = new ArrayList<>();

        // Copy all input columns
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            outputCols.add(inputSpec.getColumnSpec(i));
        }

        // Add extracted columns based on settings
        if (m_settings.isExtractYear()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "Year", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractMonth()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "Month", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractDay()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "Day", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractDayOfWeek()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "DayOfWeek", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractDayOfYear()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "DayOfYear", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractWeekOfYear()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "WeekOfYear", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractQuarter()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "Quarter", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractHour()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "Hour", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractMinute()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "Minute", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractSecond()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "Second", IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractSubsecond()) {
            final String unit = m_settings.getSubsecondUnit();
            final String unitName;
            switch (unit) {
                case "MICROSECOND":
                    unitName = "Microsecond";
                    break;
                case "NANOSECOND":
                    unitName = "Nanosecond";
                    break;
                default:
                    unitName = "Millisecond";
                    break;
            }
            outputCols.add(new DataColumnSpecCreator(prefix + unitName, IntCell.TYPE).createSpec());
        }
        if (m_settings.isExtractDayOfWeekName()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "DayOfWeekName", StringCell.TYPE).createSpec());
        }
        if (m_settings.isExtractMonthName()) {
            outputCols.add(new DataColumnSpecCreator(prefix + "MonthName", StringCell.TYPE).createSpec());
        }

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    /**
     * Returns the list of output column names that will be created based on current settings.
     */
    private List<String> getExtractedColumnNames(final String prefix) {
        final List<String> names = new ArrayList<>();
        if (m_settings.isExtractYear()) names.add(prefix + "Year");
        if (m_settings.isExtractMonth()) names.add(prefix + "Month");
        if (m_settings.isExtractDay()) names.add(prefix + "Day");
        if (m_settings.isExtractDayOfWeek()) names.add(prefix + "DayOfWeek");
        if (m_settings.isExtractDayOfYear()) names.add(prefix + "DayOfYear");
        if (m_settings.isExtractWeekOfYear()) names.add(prefix + "WeekOfYear");
        if (m_settings.isExtractQuarter()) names.add(prefix + "Quarter");
        if (m_settings.isExtractHour()) names.add(prefix + "Hour");
        if (m_settings.isExtractMinute()) names.add(prefix + "Minute");
        if (m_settings.isExtractSecond()) names.add(prefix + "Second");
        if (m_settings.isExtractSubsecond()) {
            final String unit = m_settings.getSubsecondUnit();
            switch (unit) {
                case "MICROSECOND":
                    names.add(prefix + "Microsecond");
                    break;
                case "NANOSECOND":
                    names.add(prefix + "Nanosecond");
                    break;
                default:
                    names.add(prefix + "Millisecond");
                    break;
            }
        }
        if (m_settings.isExtractDayOfWeekName()) names.add(prefix + "DayOfWeekName");
        if (m_settings.isExtractMonthName()) names.add(prefix + "MonthName");
        return names;
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
