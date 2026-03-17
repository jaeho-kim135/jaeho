package org.knime.bigdata.spark.dx.node.preproc.lagcolumn;

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
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

public class SparkLagColumnNodeModel extends SparkNodeModel {

    public static final String JOB_ID = SparkLagColumnNodeModel.class.getCanonicalName();

    private final SparkLagColumnSettings m_settings = new SparkLagColumnSettings();

    protected SparkLagColumnNodeModel() {
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

        final String col = m_settings.getColumn();
        if (col == null || col.isEmpty()) {
            throw new InvalidSettingsException("No target column selected.");
        }
        if (tableSpec.findColumnIndex(col) == -1) {
            throw new InvalidSettingsException("Target column '" + col + "' not found in input table.");
        }

        final String orderCol = m_settings.getOrderColumn();
        if (orderCol == null || orderCol.isEmpty()) {
            throw new InvalidSettingsException(
                "No order-by column selected. Spark DataFrames have no inherent row order, "
                + "so an order-by column is required for the LAG/LEAD window function.");
        }
        if (tableSpec.findColumnIndex(orderCol) == -1) {
            throw new InvalidSettingsException("Order-by column '" + orderCol + "' not found in input table.");
        }

        final int numCopies = m_settings.getNumCopies();
        if (numCopies < 1) {
            throw new InvalidSettingsException("Number of copies must be at least 1.");
        }
        if (numCopies > 100) {
            throw new InvalidSettingsException("Number of copies must not exceed 100.");
        }
        final int interval = m_settings.getLagInterval();
        if (interval < 1) {
            throw new InvalidSettingsException("Lag interval must be at least 1.");
        }
        if ((long) numCopies * interval > Integer.MAX_VALUE) {
            throw new InvalidSettingsException(
                "The product of number of copies (" + numCopies + ") and lag interval (" + interval
                + ") exceeds the maximum allowed offset (" + Integer.MAX_VALUE + ").");
        }

        for (String gc : m_settings.getGroupColumns()) {
            if (tableSpec.findColumnIndex(gc) == -1) {
                throw new InvalidSettingsException("Group column '" + gc + "' not found in input table.");
            }
        }

        // Check for output column name conflicts
        final Set<String> existing = new HashSet<>();
        for (DataColumnSpec cs : tableSpec) {
            existing.add(cs.getName());
        }

        final boolean isLag = SparkLagColumnSettings.DIR_LAG.equals(m_settings.getDirection());
        final String sign = isLag ? "-" : "+";
        for (int i = 1; i <= numCopies; i++) {
            final String newName = col + "(" + sign + (i * interval) + ")";
            if (existing.contains(newName)) {
                throw new InvalidSettingsException(
                    "Output column name '" + newName + "' already exists in the input table.");
            }
            existing.add(newName);
        }

        // Build output spec
        final DataColumnSpec srcColSpec = tableSpec.getColumnSpec(col);
        final List<DataColumnSpec> outputCols = new ArrayList<>();
        for (DataColumnSpec cs : tableSpec) {
            outputCols.add(cs);
        }
        for (int i = 1; i <= numCopies; i++) {
            final String newName = col + "(" + sign + (i * interval) + ")";
            outputCols.add(new DataColumnSpecCreator(newName, srcColSpec.getType()).createSpec());
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

        final SparkLagColumnJobInput jobInput = new SparkLagColumnJobInput(
            inputObject, outputObject,
            m_settings.getColumn(),
            m_settings.getOrderColumn(),
            m_settings.getDirection(),
            m_settings.getNumCopies(),
            m_settings.getLagInterval(),
            m_settings.getGroupColumns().toArray(new String[0]),
            m_settings.isSkipIncompleteRows());

        exec.setMessage("Executing Spark Lag Column job...");
        final SparkLagColumnJobOutput jobOutput = SparkContextUtil
            .<SparkLagColumnJobInput, SparkLagColumnJobOutput>getJobRunFactory(contextID, JOB_ID)
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
