package org.knime.bigdata.spark.dx.node.preproc.duplicates;

import java.util.List;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
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
 * Node model for the Spark Duplicate Row Filter node. Removes duplicate rows
 * using Spark Window Functions.
 */
public class SparkDuplicateRowFilterNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkDuplicateRowFilterNodeModel.class.getCanonicalName();

    private final SparkDuplicateRowFilterSettings m_settings = new SparkDuplicateRowFilterSettings();

    /** Constructor. */
    public SparkDuplicateRowFilterNodeModel() {
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

        // Validate duplicate detection columns
        final List<String> columns = m_settings.getColumns();
        if (columns == null || columns.isEmpty()) {
            setWarningMessage("No duplicate detection columns selected. All columns will be used.");
        } else {
            for (String col : columns) {
                if (tableSpec.findColumnIndex(col) == -1) {
                    throw new InvalidSettingsException(
                        "Duplicate detection column '" + col + "' not found in input table.");
                }
            }
        }

        final String rowSelection = m_settings.getRowSelection();

        // Check if order column is needed
        final boolean needsOrderColumn = "FIRST".equals(rowSelection) || "LAST".equals(rowSelection)
            || "MINIMUM".equals(rowSelection) || "MAXIMUM".equals(rowSelection);

        if (needsOrderColumn) {
            String orderColumn = m_settings.getOrderColumn();
            if (orderColumn == null || orderColumn.trim().isEmpty()) {
                // Auto-select the first column as order column
                if (tableSpec.getNumColumns() > 0) {
                    orderColumn = tableSpec.getColumnSpec(0).getName();
                    m_settings.setOrderColumn(orderColumn);
                    setWarningMessage("Order column auto-selected: '" + orderColumn + "'");
                } else {
                    throw new InvalidSettingsException("No columns available for order column.");
                }
            }
            if (tableSpec.findColumnIndex(orderColumn) == -1) {
                throw new InvalidSettingsException(
                    "Order column '" + orderColumn + "' not found in input table.");
            }
        }

        DataTableSpec outputSpec = tableSpec;
        if ("KEEP".equals(m_settings.getDuplicateHandling()) && m_settings.isAddStatusColumn()) {
            outputSpec = new DataTableSpec(tableSpec,
                new DataTableSpec(new DataColumnSpecCreator(
                    m_settings.getStatusColumnName(), StringCell.TYPE).createSpec()));
        }
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> columns = m_settings.getColumns();
        final String[] dupColumns = (columns != null && !columns.isEmpty())
            ? columns.toArray(new String[0]) : new String[0];

        final SparkDuplicateRowFilterJobInput jobInput = new SparkDuplicateRowFilterJobInput(
            inputObject,
            outputObject,
            dupColumns,
            m_settings.getDuplicateHandling(),
            m_settings.getRowSelection(),
            m_settings.getOrderColumn(),
            m_settings.getOrderDirection(),
            m_settings.isAddStatusColumn(),
            m_settings.getStatusColumnName());

        exec.setMessage("Executing Spark duplicate row filter job...");
        final SparkDuplicateRowFilterJobOutput jobOutput = SparkContextUtil
            .<SparkDuplicateRowFilterJobInput, SparkDuplicateRowFilterJobOutput>getJobRunFactory(contextID, JOB_ID)
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
