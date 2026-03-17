package org.knime.bigdata.spark.dx.node.convert.datetimetostring;

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
 * Node model for the Spark Date&amp;Time to String node.
 * Converts date/time columns to string columns using Spark's {@code date_format()} function.
 */
public class SparkDateTimeToStringNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkDateTimeToStringNodeModel.class.getCanonicalName();

    private final SparkDateTimeToStringSettings m_settings = new SparkDateTimeToStringSettings();

    /** Constructor. */
    public SparkDateTimeToStringNodeModel() {
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

        // Validate settings against the input table spec
        validateConfiguration(tableSpec);

        // Build output spec (output type is always String, so we can pre-compute)
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> columns = m_settings.getColumns();

        final SparkDateTimeToStringJobInput jobInput = new SparkDateTimeToStringJobInput(
            inputObject, outputObject,
            columns.toArray(new String[0]),
            m_settings.getFormat(),
            m_settings.getLocale(),
            m_settings.isReplace(),
            m_settings.getSuffix());

        exec.setMessage("Executing Spark date/time to string job...");
        final SparkDateTimeToStringJobOutput jobOutput = SparkContextUtil
            .<SparkDateTimeToStringJobInput, SparkDateTimeToStringJobOutput>getJobRunFactory(contextID, JOB_ID)
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
        final List<String> columns = m_settings.getColumns();

        // Validate columns selected
        if (columns == null || columns.isEmpty()) {
            setWarningMessage("No columns selected. Output will be identical to input.");
            return;
        }

        // Validate selected columns exist in input
        for (final String col : columns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException(
                    "Selected column '" + col + "' does not exist in the input table.");
            }
        }

        // Validate format not empty
        final String format = m_settings.getFormat();
        if (format == null || format.trim().isEmpty()) {
            throw new InvalidSettingsException(
                "Date format pattern must not be empty.");
        }

        // Validate APPEND mode settings
        if (!m_settings.isReplace()) {
            final String suffix = m_settings.getSuffix();
            if (suffix == null || suffix.isEmpty()) {
                throw new InvalidSettingsException(
                    "Column suffix must not be empty in append mode.");
            }

            // Check for output column name conflicts
            final Set<String> existingColumns = new HashSet<>();
            for (int i = 0; i < tableSpec.getNumColumns(); i++) {
                existingColumns.add(tableSpec.getColumnSpec(i).getName());
            }
            for (final String col : columns) {
                final String newName = col + suffix;
                if (existingColumns.contains(newName)) {
                    throw new InvalidSettingsException(
                        "Output column name '" + newName + "' conflicts with an existing column. "
                        + "Please choose a different suffix.");
                }
            }
        }
    }

    /**
     * Creates the output DataTableSpec.
     * In REPLACE mode, selected columns are changed to StringCell.TYPE.
     * In APPEND mode, new StringCell.TYPE columns are added with the configured suffix.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final List<String> columns = m_settings.getColumns();
        if (columns == null || columns.isEmpty()) {
            return inputSpec;
        }

        final Set<String> selectedCols = new HashSet<>(columns);
        final List<DataColumnSpec> outputCols = new ArrayList<>();

        if (m_settings.isReplace()) {
            // Replace: change selected column types to String
            for (int i = 0; i < inputSpec.getNumColumns(); i++) {
                final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
                if (selectedCols.contains(colSpec.getName())) {
                    outputCols.add(new DataColumnSpecCreator(colSpec.getName(), StringCell.TYPE).createSpec());
                } else {
                    outputCols.add(colSpec);
                }
            }
        } else {
            // Append: keep original columns + add new String columns
            for (int i = 0; i < inputSpec.getNumColumns(); i++) {
                outputCols.add(inputSpec.getColumnSpec(i));
            }
            final String suffix = m_settings.getSuffix();
            for (final String col : columns) {
                final String newName = col + suffix;
                outputCols.add(new DataColumnSpecCreator(newName, StringCell.TYPE).createSpec());
            }
        }

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
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
