package org.knime.bigdata.spark.dx.node.preproc.stringtonumber;

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
import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
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
 * Node model for the Spark String to Number node. Converts String columns to
 * numeric types (Integer, Double, Long) using a Spark DataFrame job.
 */
public class SparkStringToNumberNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkStringToNumberNodeModel.class.getCanonicalName();

    private final SparkStringToNumberSettings m_settings = new SparkStringToNumberSettings();

    /** Constructor. */
    public SparkStringToNumberNodeModel() {
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

        // Validate included columns
        final List<String> inclCols = m_settings.getIncludedColumns();
        if (inclCols.isEmpty()) {
            throw new InvalidSettingsException("No columns selected.");
        }

        for (final String col : inclCols) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Column '" + col + "' not found in input table.");
            }
        }

        // Validate decimal separator
        final String decSep = m_settings.getDecimalSeparator();
        if (decSep.length() > 1) {
            throw new InvalidSettingsException("Decimal separator must be at most one character.");
        }

        // Validate thousands separator
        final String thousSep = m_settings.getThousandsSeparator();
        if (thousSep.length() > 1) {
            throw new InvalidSettingsException("Thousands separator must be at most one character.");
        }

        // Validate decimal != thousands
        if (!decSep.isEmpty() && !thousSep.isEmpty() && decSep.equals(thousSep)) {
            throw new InvalidSettingsException("Decimal separator and thousands separator must be different.");
        }

        // Build output spec: replace selected columns' types with target numeric type
        final DataTableSpec outputSpec = createOutputSpec(tableSpec, inclCols, m_settings.getParseType());
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    private static DataTableSpec createOutputSpec(final DataTableSpec inputSpec, final List<String> inclCols,
            final String parseType) {
        final DataType targetType;
        if ("INTEGER".equals(parseType)) {
            targetType = IntCell.TYPE;
        } else if ("LONG".equals(parseType)) {
            targetType = LongCell.TYPE;
        } else {
            targetType = DoubleCell.TYPE;
        }

        final List<DataColumnSpec> outputCols = new ArrayList<>();
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
            if (inclCols.contains(colSpec.getName())) {
                outputCols.add(new DataColumnSpecCreator(colSpec.getName(), targetType).createSpec());
            } else {
                outputCols.add(colSpec);
            }
        }
        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> inclCols = m_settings.getIncludedColumns();

        final SparkStringToNumberJobInput jobInput = new SparkStringToNumberJobInput(
            inputObject,
            outputObject,
            inclCols.toArray(new String[0]),
            m_settings.getParseType(),
            m_settings.getDecimalSeparator(),
            m_settings.getThousandsSeparator(),
            m_settings.isGenericParse(),
            m_settings.isFailOnError());

        exec.setMessage("Executing Spark String to Number job...");
        final SparkStringToNumberJobOutput jobOutput = SparkContextUtil
            .<SparkStringToNumberJobInput, SparkStringToNumberJobOutput>getJobRunFactory(contextID, JOB_ID)
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
