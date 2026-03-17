package org.knime.bigdata.spark.dx.node.preproc.stringtodatetime;

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
 * Node model for the Spark String to Date&Time node. Converts string columns to
 * date/time types using a Spark DataFrame job.
 */
public class SparkStringToDateTimeNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkStringToDateTimeNodeModel.class.getCanonicalName();

    private final SparkStringToDateTimeSettings m_settings = new SparkStringToDateTimeSettings();

    /** Constructor. */
    public SparkStringToDateTimeNodeModel() {
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

        final List<String> inclCols = m_settings.getIncludedColumns();
        if (inclCols.isEmpty()) {
            throw new InvalidSettingsException("No columns selected.");
        }

        final String format = m_settings.getFormat();
        if (format == null || format.isBlank()) {
            throw new InvalidSettingsException("Date&Time format must not be empty.");
        }

        for (final String col : inclCols) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Column '" + col + "' not found in input table.");
            }
        }

        // Output column types depend on how Spark's DateType/TimestampType map to KNIME types
        // via the type converter. The exact output spec will be determined at execution time
        // from the Spark job result, so we return null here.
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkStringToDateTimeJobInput jobInput = new SparkStringToDateTimeJobInput(
            inputObject,
            outputObject,
            m_settings.getIncludedColumns().toArray(new String[0]),
            m_settings.getFormat(),
            m_settings.getOutputType(),
            m_settings.getLocale(),
            m_settings.isFailOnError());

        exec.setMessage("Executing Spark String to Date&Time job...");
        final SparkStringToDateTimeJobOutput jobOutput = SparkContextUtil
            .<SparkStringToDateTimeJobInput, SparkStringToDateTimeJobOutput>getJobRunFactory(contextID, JOB_ID)
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
