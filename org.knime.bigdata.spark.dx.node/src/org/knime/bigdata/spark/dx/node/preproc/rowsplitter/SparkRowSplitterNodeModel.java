package org.knime.bigdata.spark.dx.node.preproc.rowsplitter;

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
 * Node model for the Spark Row Splitter node.
 * Splits input rows into two outputs (Matches and Non-Matches) based on filter conditions.
 */
public class SparkRowSplitterNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkRowSplitterNodeModel.class.getCanonicalName();

    private final SparkRowSplitterSettings m_settings = new SparkRowSplitterSettings();

    /** Constructor with 1 input and 2 outputs. */
    protected SparkRowSplitterNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame available.");
        }

        if (!m_settings.isNodeConfigured()) {
            throw new InvalidSettingsException(
                "Node has not been configured. Open the dialog and define at least one filter condition.");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        validatePredicates(tableSpec);

        // Both output ports have the same schema as the input
        return new PortObjectSpec[]{inSpecs[0], inSpecs[0]};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String matchOutputObject = SparkIDs.createSparkDataObjectID();
        final String nonMatchOutputObject = SparkIDs.createSparkDataObjectID();

        final List<SparkRowSplitterSettings.Predicate> predicates = m_settings.getPredicates();

        // Build parallel arrays for job input
        final String[] columns = new String[predicates.size()];
        final String[] operators = new String[predicates.size()];
        final String[] values = new String[predicates.size()];
        final String[] upperValues = new String[predicates.size()];
        final boolean[] caseSensitives = new boolean[predicates.size()];

        for (int i = 0; i < predicates.size(); i++) {
            final SparkRowSplitterSettings.Predicate pred = predicates.get(i);
            columns[i] = pred.getColumn();
            operators[i] = pred.getOperator();
            values[i] = pred.getValue() != null ? pred.getValue() : "";
            upperValues[i] = pred.getUpperValue() != null ? pred.getUpperValue() : "";
            caseSensitives[i] = pred.isCaseSensitive();
        }

        final SparkRowSplitterJobInput jobInput = new SparkRowSplitterJobInput(
            inputObject, matchOutputObject, nonMatchOutputObject,
            m_settings.getMatchCriteria(),
            columns, operators, values, upperValues, caseSensitives);

        exec.setMessage("Executing Spark Row Splitter job...");
        final SparkRowSplitterJobOutput jobOutput = SparkContextUtil
            .<SparkRowSplitterJobInput, SparkRowSplitterJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        // Build match output
        final DataTableSpec matchSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(matchOutputObject));
        final SparkDataTable matchTable = new SparkDataTable(contextID, matchOutputObject, matchSpec);

        // Build non-match output
        final DataTableSpec nonMatchSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(nonMatchOutputObject));
        final SparkDataTable nonMatchTable = new SparkDataTable(contextID, nonMatchOutputObject, nonMatchSpec);

        return new PortObject[]{
            new SparkDataPortObject(matchTable),
            new SparkDataPortObject(nonMatchTable)
        };
    }

    /**
     * Validates the filter predicates against the input table spec.
     */
    private void validatePredicates(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final List<SparkRowSplitterSettings.Predicate> predicates = m_settings.getPredicates();

        if (predicates.isEmpty()) {
            throw new InvalidSettingsException("At least one filter condition is required.");
        }

        for (int i = 0; i < predicates.size(); i++) {
            final SparkRowSplitterSettings.Predicate pred = predicates.get(i);

            // Check column exists
            if (pred.getColumn() == null || pred.getColumn().trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Condition " + (i + 1) + ": column is not selected.");
            }
            if (tableSpec.findColumnIndex(pred.getColumn()) == -1) {
                throw new InvalidSettingsException(
                    "Condition " + (i + 1) + ": column '" + pred.getColumn()
                    + "' not found in the input table.");
            }

            final String operator = pred.getOperator();

            // Value required for non-nullary operators
            if (!SparkRowSplitterSettings.isNullaryOperator(operator)) {
                if (pred.getValue() == null || pred.getValue().trim().isEmpty()) {
                    throw new InvalidSettingsException(
                        "Condition " + (i + 1) + ": value is empty.");
                }
            }

            // Upper value required for BETWEEN
            if ("BETWEEN".equals(operator)) {
                if (pred.getUpperValue() == null || pred.getUpperValue().trim().isEmpty()) {
                    throw new InvalidSettingsException(
                        "Condition " + (i + 1) + ": upper value is required for BETWEEN operator.");
                }
            }
        }
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
