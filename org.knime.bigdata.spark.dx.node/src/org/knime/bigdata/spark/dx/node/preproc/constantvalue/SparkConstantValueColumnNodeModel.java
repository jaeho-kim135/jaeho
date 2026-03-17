package org.knime.bigdata.spark.dx.node.preproc.constantvalue;

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
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Constant Value Column node.
 * Adds or replaces a column with a constant value using Spark's lit() function.
 */
public class SparkConstantValueColumnNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkConstantValueColumnNodeModel.class.getCanonicalName();

    private final SparkConstantValueColumnSettings m_settings = new SparkConstantValueColumnSettings();

    /** Constructor. */
    public SparkConstantValueColumnNodeModel() {
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

        // Validate settings against the input table spec
        validateConfiguration(tableSpec);

        // Build output spec
        final DataType outputType = mapValueTypeToKNIMEType(m_settings.getValueType());
        if (outputType == null) {
            // DATE/TIMESTAMP types: let the job determine the spec
            return new PortObjectSpec[]{null};
        }

        final DataTableSpec outputSpec = createOutputSpec(tableSpec, outputType);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkConstantValueColumnJobInput jobInput = new SparkConstantValueColumnJobInput(
            inputObject, outputObject,
            m_settings.getEffectiveColumnName(),
            m_settings.getValueType(),
            m_settings.getValue(),
            m_settings.isMissing(),
            m_settings.isReplace());

        exec.setMessage("Executing Spark constant value column job...");
        final SparkConstantValueColumnJobOutput jobOutput = SparkContextUtil
            .<SparkConstantValueColumnJobInput, SparkConstantValueColumnJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Validates the current configuration against the input table spec.
     */
    private void validateConfiguration(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final boolean isReplace = m_settings.isReplace();
        final String effectiveColName = m_settings.getEffectiveColumnName();

        if (isReplace) {
            // REPLACE mode: the replace column must be specified and exist
            final String replaceCol = m_settings.getReplaceColumn();
            if (replaceCol == null || replaceCol.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "No replacement column selected. Please select a column to replace.");
            }
            if (tableSpec.findColumnIndex(replaceCol) == -1) {
                throw new InvalidSettingsException(
                    "Replacement column '" + replaceCol + "' does not exist in the input table.");
            }
        } else {
            // APPEND mode: column name must not be empty and must not conflict
            final String colName = m_settings.getColumnName();
            if (colName == null || colName.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Column name must not be empty.");
            }
            if (tableSpec.findColumnIndex(colName) != -1) {
                throw new InvalidSettingsException(
                    "Column '" + colName + "' already exists in the input table. "
                    + "Use Replace mode or choose a different column name.");
            }
        }

        // Validate value if not missing
        if (!m_settings.isMissing()) {
            final String value = m_settings.getValue();
            if (value == null) {
                throw new InvalidSettingsException(
                    "Value must not be null. Enter a constant value or enable 'Use missing value'.");
            }
            // Empty string is valid for STRING type, but not for numeric/date types
            if (value.trim().isEmpty() && !"STRING".equals(m_settings.getValueType())) {
                throw new InvalidSettingsException(
                    "Value must not be empty for type " + m_settings.getValueType()
                    + ". Enter a constant value or enable 'Use missing value'.");
            }
            validateValueForType(value, m_settings.getValueType());
        }
    }

    /**
     * Validates that the given value string is parseable for the specified type.
     */
    private static void validateValueForType(final String value, final String valueType)
            throws InvalidSettingsException {
        switch (valueType) {
            case "INTEGER":
                try {
                    Integer.parseInt(value.trim());
                } catch (final NumberFormatException e) {
                    throw new InvalidSettingsException(
                        "Value '" + value + "' is not a valid integer.");
                }
                break;
            case "LONG":
                try {
                    Long.parseLong(value.trim());
                } catch (final NumberFormatException e) {
                    throw new InvalidSettingsException(
                        "Value '" + value + "' is not a valid long.");
                }
                break;
            case "DOUBLE":
                try {
                    Double.parseDouble(value.trim());
                } catch (final NumberFormatException e) {
                    throw new InvalidSettingsException(
                        "Value '" + value + "' is not a valid double.");
                }
                break;
            case "BOOLEAN":
                if (!"true".equalsIgnoreCase(value.trim()) && !"false".equalsIgnoreCase(value.trim())) {
                    throw new InvalidSettingsException(
                        "Value '" + value + "' is not a valid boolean. Use 'true' or 'false'.");
                }
                break;
            case "STRING":
            case "DATE":
            case "TIMESTAMP":
                // No parse validation at configure time (Spark handles format)
                break;
            default:
                throw new InvalidSettingsException(
                    "Unknown value type: '" + valueType + "'. Supported types: "
                    + "STRING, INTEGER, LONG, DOUBLE, BOOLEAN, DATE, TIMESTAMP.");
        }
    }

    /**
     * Maps the value type string to the corresponding KNIME DataType.
     * Returns null for DATE and TIMESTAMP (output spec determined by job).
     */
    private static DataType mapValueTypeToKNIMEType(final String valueType) {
        switch (valueType) {
            case "STRING":
                return StringCell.TYPE;
            case "INTEGER":
                return IntCell.TYPE;
            case "LONG":
                return LongCell.TYPE;
            case "DOUBLE":
                return DoubleCell.TYPE;
            case "BOOLEAN":
                return BooleanCell.TYPE;
            case "DATE":
            case "TIMESTAMP":
                // Let the Spark job determine the exact type mapping
                return null;
            default:
                return null;
        }
    }

    /**
     * Creates the output DataTableSpec by adding or replacing a column.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec, final DataType outputType) {
        final String effectiveColName = m_settings.getEffectiveColumnName();
        final DataColumnSpec newColSpec =
            new DataColumnSpecCreator(effectiveColName, outputType).createSpec();

        if (m_settings.isReplace()) {
            // Replace: rebuild spec with the same column name but new type
            final List<DataColumnSpec> cols = new ArrayList<>();
            for (int i = 0; i < inputSpec.getNumColumns(); i++) {
                final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
                if (colSpec.getName().equals(effectiveColName)) {
                    cols.add(newColSpec);
                } else {
                    cols.add(colSpec);
                }
            }
            return new DataTableSpec(cols.toArray(new DataColumnSpec[0]));
        } else {
            // Append: add new column at the end
            final List<DataColumnSpec> cols = new ArrayList<>();
            for (int i = 0; i < inputSpec.getNumColumns(); i++) {
                cols.add(inputSpec.getColumnSpec(i));
            }
            cols.add(newColSpec);
            return new DataTableSpec(cols.toArray(new DataColumnSpec[0]));
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
