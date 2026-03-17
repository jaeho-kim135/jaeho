package org.knime.bigdata.spark.dx.node.preproc.numbertostring;

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
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Number to String node. Converts numeric columns to
 * String type using a Spark DataFrame job.
 */
public class SparkNumberToStringNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkNumberToStringNodeModel.class.getCanonicalName();

    private final SparkNumberToStringSettings m_settings = new SparkNumberToStringSettings();

    /** Constructor. */
    public SparkNumberToStringNodeModel() {
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

        for (final String col : inclCols) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Column '" + col + "' not found in input table.");
            }
        }

        // Build output spec: replace selected columns' types with StringCell.TYPE
        final List<DataColumnSpec> outputCols = new ArrayList<>();
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = tableSpec.getColumnSpec(i);
            if (inclCols.contains(colSpec.getName())) {
                outputCols.add(new DataColumnSpecCreator(colSpec.getName(), StringCell.TYPE).createSpec());
            } else {
                outputCols.add(colSpec);
            }
        }
        final DataTableSpec outputSpec = new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkNumberToStringJobInput jobInput = new SparkNumberToStringJobInput(
            inputObject,
            outputObject,
            m_settings.getIncludedColumns().toArray(new String[0]));

        exec.setMessage("Executing Spark Number to String job...");
        final SparkNumberToStringJobOutput jobOutput = SparkContextUtil
            .<SparkNumberToStringJobInput, SparkNumberToStringJobOutput>getJobRunFactory(contextID, JOB_ID)
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
