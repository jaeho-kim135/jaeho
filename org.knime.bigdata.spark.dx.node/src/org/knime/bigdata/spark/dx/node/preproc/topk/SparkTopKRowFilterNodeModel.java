package org.knime.bigdata.spark.dx.node.preproc.topk;

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
 * Node model for the Spark Top k Row Filter node. Selects the top k rows
 * based on one or more sort criteria, optionally per group.
 */
public class SparkTopKRowFilterNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkTopKRowFilterNodeModel.class.getCanonicalName();

    private final SparkTopKRowFilterSettings m_settings = new SparkTopKRowFilterSettings();

    /** Constructor. */
    public SparkTopKRowFilterNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame/RDD available.");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        // Validate sort columns
        final String[] sortCols = m_settings.getSortColumns();
        if (sortCols == null || sortCols.length == 0) {
            throw new InvalidSettingsException("At least one sort column must be selected.");
        }
        for (int i = 0; i < sortCols.length; i++) {
            final String col = sortCols[i];
            if (col == null || col.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Sort criterion " + (i + 1) + ": column must be selected.");
            }
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException(
                    "Sort column '" + col + "' not found in input table.");
            }
        }

        // Validate k
        final long k = m_settings.getK();
        if (k < 1) {
            throw new InvalidSettingsException("k must be at least 1.");
        }

        // Validate k > Integer.MAX_VALUE constraint when no groups
        final List<String> groupCols = m_settings.getGroupColumns();
        if (k > Integer.MAX_VALUE
                && (groupCols == null || groupCols.isEmpty())) {
            throw new InvalidSettingsException(
                "k exceeds Integer.MAX_VALUE (" + Integer.MAX_VALUE
                + "). Dataset.limit() only accepts int values. "
                + "Use group columns to avoid this limitation.");
        }

        // Validate group columns exist in input spec
        if (groupCols != null) {
            for (String col : groupCols) {
                if (tableSpec.findColumnIndex(col) == -1) {
                    throw new InvalidSettingsException(
                        "Group column '" + col + "' not found in input table.");
                }
            }
        }

        // Output spec is the same as input spec (filtering doesn't change schema)
        return new PortObjectSpec[]{sparkSpec};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkTopKRowFilterJobInput jobInput = new SparkTopKRowFilterJobInput(
            inputObject, outputObject,
            m_settings.getK(),
            m_settings.getFilterMode(),
            m_settings.getOutputOrder(),
            m_settings.isMissingsToEnd(),
            m_settings.getGroupColumns().toArray(new String[0]),
            m_settings.getSortColumns(),
            m_settings.getSortOrders());

        exec.setMessage("Executing Spark Top k Row Filter job...");
        final SparkTopKRowFilterJobOutput jobOutput = SparkContextUtil
            .<SparkTopKRowFilterJobInput, SparkTopKRowFilterJobOutput>getJobRunFactory(contextID, JOB_ID)
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
