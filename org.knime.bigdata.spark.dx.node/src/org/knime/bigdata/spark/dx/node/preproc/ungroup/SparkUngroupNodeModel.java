package org.knime.bigdata.spark.dx.node.preproc.ungroup;

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
 * Node model for the Spark Ungroup node. Explodes array, map, or delimited string
 * columns into individual rows using Spark's explode() function.
 */
public class SparkUngroupNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkUngroupNodeModel.class.getCanonicalName();

    private final SparkUngroupSettings m_settings = new SparkUngroupSettings();

    /** Constructor. */
    public SparkUngroupNodeModel() {
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

        // Validate target columns
        final List<String> columns = m_settings.getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new InvalidSettingsException("No target columns selected. "
                + "Please select at least one column to ungroup.");
        }

        for (String col : columns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Target column '" + col + "' not found in input table.");
            }
        }

        // Validate delimiter for STRING_SPLIT mode
        if ("STRING_SPLIT".equals(m_settings.getExplodeMode())) {
            final String delimiter = m_settings.getDelimiter();
            if (delimiter == null || delimiter.isEmpty()) {
                throw new InvalidSettingsException(
                    "Delimiter must not be empty in 'Split string by delimiter' mode.");
            }
        }

        // Return null spec — explode result type depends on input column types
        // and is determined at execution time
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> columns = m_settings.getColumns();

        final SparkUngroupJobInput jobInput = new SparkUngroupJobInput(
            inputObject,
            outputObject,
            columns.toArray(new String[0]),
            m_settings.getExplodeMode(),
            m_settings.getDelimiter(),
            m_settings.isRemoveOriginal(),
            m_settings.isSkipNulls(),
            m_settings.isSkipEmpty());

        exec.setMessage("Executing Spark ungroup job...");
        final SparkUngroupJobOutput jobOutput = SparkContextUtil
            .<SparkUngroupJobInput, SparkUngroupJobOutput>getJobRunFactory(contextID, JOB_ID)
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
