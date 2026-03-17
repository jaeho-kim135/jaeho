package org.knime.bigdata.spark.dx.node.preproc.rank;

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
import org.knime.core.data.def.LongCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Rank node. Computes rank values using
 * Spark SQL window functions (RANK, DENSE_RANK, ROW_NUMBER).
 */
public class SparkRankNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkRankNodeModel.class.getCanonicalName();

    private final SparkRankSettings m_settings = new SparkRankSettings();

    /** Constructor. */
    public SparkRankNodeModel() {
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

        // Validate ranking columns
        final String[] rankingColumns = m_settings.getRankingColumns();
        if (rankingColumns == null || rankingColumns.length == 0) {
            throw new InvalidSettingsException("No ranking columns specified. "
                + "Please add at least one ranking criterion.");
        }

        for (String col : rankingColumns) {
            if (col == null || col.isEmpty()) {
                throw new InvalidSettingsException("Ranking column name must not be empty.");
            }
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Ranking column '" + col + "' not found in input table.");
            }
        }

        // Validate group columns
        final List<String> groupColumns = m_settings.getGroupColumns();
        if (groupColumns != null) {
            for (String col : groupColumns) {
                if (tableSpec.findColumnIndex(col) == -1) {
                    throw new InvalidSettingsException("Group column '" + col + "' not found in input table.");
                }
            }
        }

        // Validate output column name
        final String outputColName = m_settings.getOutputColName();
        if (outputColName == null || outputColName.trim().isEmpty()) {
            throw new InvalidSettingsException("Output column name must not be empty.");
        }

        // Check for output column name conflict with existing columns
        if (tableSpec.findColumnIndex(outputColName.trim()) != -1) {
            throw new InvalidSettingsException(
                "Output column name '" + outputColName.trim() + "' conflicts with an existing column. "
                + "Please choose a different name.");
        }

        // Build output spec
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    /**
     * Creates the output table spec by appending the rank column to the input spec.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final List<DataColumnSpec> outputCols = new ArrayList<>();

        // Add all input columns
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            outputCols.add(inputSpec.getColumnSpec(i));
        }

        // Add rank column
        final String outputColName = m_settings.getOutputColName().trim();
        final boolean useLong = "LONG".equals(m_settings.getRankDataType());
        outputCols.add(new DataColumnSpecCreator(outputColName,
            useLong ? LongCell.TYPE : IntCell.TYPE).createSpec());

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> groupColumns = m_settings.getGroupColumns();

        final SparkRankJobInput jobInput = new SparkRankJobInput(
            inputObject,
            outputObject,
            m_settings.getRankingColumns(),
            m_settings.getRankingOrders(),
            groupColumns != null ? groupColumns.toArray(new String[0]) : new String[0],
            m_settings.getRankMode(),
            m_settings.getOutputColName().trim(),
            m_settings.getRankDataType(),
            m_settings.isMissingToEnd());

        exec.setMessage("Executing Spark rank job...");
        final SparkRankJobOutput jobOutput = SparkContextUtil
            .<SparkRankJobInput, SparkRankJobOutput>getJobRunFactory(contextID, JOB_ID)
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
