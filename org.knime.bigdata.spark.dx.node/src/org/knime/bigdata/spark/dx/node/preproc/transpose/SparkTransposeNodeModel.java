package org.knime.bigdata.spark.dx.node.preproc.transpose;

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
 * Node model for the Spark Table Transposer node. Transposes a Spark DataFrame
 * by collecting data to the driver and reconstructing it with rows and columns swapped.
 */
public class SparkTransposeNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkTransposeNodeModel.class.getCanonicalName();

    private final SparkTransposeSettings m_settings = new SparkTransposeSettings();

    /** Constructor. */
    protected SparkTransposeNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame/RDD available");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        // Validate maxRows
        final int maxRows = m_settings.getMaxRows();
        if (maxRows < 1) {
            throw new InvalidSettingsException("Maximum rows must be at least 1.");
        }
        if (maxRows > 100000) {
            throw new InvalidSettingsException(
                "Maximum rows must not exceed 100,000. Transpose collects all data to the driver.");
        }

        // Validate idColumn if specified
        final String idColumn = m_settings.getIdColumn();
        if (idColumn != null && !idColumn.isEmpty()) {
            if (tableSpec.findColumnIndex(idColumn) == -1) {
                throw new InvalidSettingsException(
                    "ID column '" + idColumn + "' not found in input table.");
            }
        }

        // Set warning about collecting data to driver
        setWarningMessage("This node collects all data to the driver. Only suitable for small datasets.");

        // Output spec depends on input row values, cannot be determined at configure time
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkTransposeJobInput jobInput = new SparkTransposeJobInput(
            inputObject,
            outputObject,
            m_settings.getMaxRows(),
            m_settings.getIdColumn());

        exec.setMessage("Executing Spark transpose job...");
        final SparkTransposeJobOutput jobOutput = SparkContextUtil
            .<SparkTransposeJobInput, SparkTransposeJobOutput>getJobRunFactory(contextID, JOB_ID)
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
