package org.knime.bigdata.spark.dx.node.preproc.editcolumn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
 * Node model for the Spark Edit Column node.
 * Renames columns, changes data types, and reorders columns using Spark's select/cast/as.
 */
public class SparkEditColumnNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkEditColumnNodeModel.class.getCanonicalName();

    private final SparkEditColumnSettings m_settings = new SparkEditColumnSettings();

    /** Constructor. */
    public SparkEditColumnNodeModel() {
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

        // Filter out columns that no longer exist in the input
        final FilteredColumns filtered = filterExistingColumns(
            m_settings.getSourceColumns(), m_settings.getNewNames(), m_settings.getNewTypes(), tableSpec);

        if (!filtered.missingColumns.isEmpty()) {
            setWarningMessage("Skipped missing column(s): " + String.join(", ", filtered.missingColumns)
                + ". Re-open the dialog to update settings.");
        }

        // Validate (returns early if empty)
        validateEditConfiguration(filtered.sourceColumns, filtered.newNames, filtered.newTypes, tableSpec);
        if (filtered.sourceColumns.length == 0) {
            // Pass through: output spec equals input spec
            return new PortObjectSpec[]{inSpecs[0]};
        }

        // Build output spec
        final DataTableSpec outputSpec = buildOutputSpec(
            filtered.sourceColumns, filtered.newNames, filtered.newTypes, tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];

        final DataTableSpec tableSpec = sparkPort.getTableSpec();

        // Filter out columns that no longer exist in the input
        final FilteredColumns filtered = filterExistingColumns(
            m_settings.getSourceColumns(), m_settings.getNewNames(), m_settings.getNewTypes(), tableSpec);

        if (!filtered.missingColumns.isEmpty()) {
            setWarningMessage("Skipped missing column(s): " + String.join(", ", filtered.missingColumns)
                + ". Re-open the dialog to update settings.");
        }

        if (filtered.sourceColumns.length == 0) {
            setWarningMessage("No column edits configured — passing through input unchanged.");
            return new PortObject[]{sparkPort};
        }

        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final SparkEditColumnJobInput jobInput = new SparkEditColumnJobInput(
            inputObject, outputObject,
            filtered.sourceColumns,
            filtered.newNames,
            filtered.newTypes);

        exec.setMessage("Executing Spark edit column job...");
        final SparkEditColumnJobOutput jobOutput = SparkContextUtil
            .<SparkEditColumnJobInput, SparkEditColumnJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Validates the edit configuration against the input table spec.
     */
    private void validateEditConfiguration(final String[] sourceColumns, final String[] newNames,
            final String[] newTypes, final DataTableSpec tableSpec) throws InvalidSettingsException {

        if (sourceColumns.length == 0) {
            setWarningMessage("No columns configured. Open the dialog and add at least one column edit.");
            return;
        }

        final Set<String> seenSources = new HashSet<>();
        final Set<String> outputNames = new LinkedHashSet<>();

        for (int i = 0; i < sourceColumns.length; i++) {
            final String src = sourceColumns[i];
            if (src == null || src.isEmpty()) {
                throw new InvalidSettingsException(
                    "Row " + (i + 1) + ": Source column must be selected.");
            }
            if (tableSpec.findColumnIndex(src) == -1) {
                throw new InvalidSettingsException(
                    "Row " + (i + 1) + ": Source column '" + src + "' does not exist in the input.");
            }
            if (!seenSources.add(src)) {
                throw new InvalidSettingsException(
                    "Column '" + src + "' is referenced multiple times.");
            }

            // Determine effective output name
            final String newName = (i < newNames.length) ? newNames[i] : "";
            final String effectiveName = (newName == null || newName.isEmpty()) ? src : newName;
            if (effectiveName.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Row " + (i + 1) + ": Column name must not be blank (use empty for original name).");
            }
            if (!outputNames.add(effectiveName)) {
                throw new InvalidSettingsException(
                    "Output column name '" + effectiveName + "' is duplicated.");
            }
        }

        // Check for conflicts with unedited columns
        final Map<String, String> outputNameToSource = new LinkedHashMap<>();
        for (int i = 0; i < sourceColumns.length; i++) {
            final String nn = (i < newNames.length) ? newNames[i] : "";
            final String eff = (nn == null || nn.isEmpty()) ? sourceColumns[i] : nn;
            outputNameToSource.put(eff, sourceColumns[i]);
        }
        final Set<String> editedSources = new HashSet<>(seenSources);
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final String origCol = tableSpec.getColumnSpec(i).getName();
            if (!editedSources.contains(origCol)) {
                if (outputNames.contains(origCol)) {
                    final String renamedFrom = outputNameToSource.get(origCol);
                    throw new InvalidSettingsException(
                        "Column '" + renamedFrom + "' renamed to '" + origCol
                            + "' conflicts with unedited input column '" + origCol + "'.");
                }
            }
        }
    }

    /**
     * Builds the output DataTableSpec based on the edit configuration.
     */
    private DataTableSpec buildOutputSpec(final String[] sourceColumns, final String[] newNames,
            final String[] newTypes, final DataTableSpec tableSpec) {

        // Build maps for edits
        final Map<String, String> renameMap = new LinkedHashMap<>();
        final Map<String, String> castMap = new LinkedHashMap<>();
        final Set<String> editedOriginals = new LinkedHashSet<>();

        for (int i = 0; i < sourceColumns.length; i++) {
            final String src = sourceColumns[i];
            editedOriginals.add(src);
            final String newName = (i < newNames.length && newNames[i] != null && !newNames[i].isEmpty())
                ? newNames[i] : src;
            renameMap.put(src, newName);
            if (i < newTypes.length && !"KEEP".equals(newTypes[i])) {
                castMap.put(src, newTypes[i]);
            }
        }

        final List<DataColumnSpec> outputCols = new ArrayList<>();

        // 1. Columns from settings (in configured order)
        for (final String src : sourceColumns) {
            final String outName = renameMap.get(src);
            DataType outType;
            if (castMap.containsKey(src)) {
                outType = mapSparkTypeToKNIME(castMap.get(src));
                if (outType == null) {
                    // DATE/TIMESTAMP: use original type (job will determine exact mapping)
                    outType = tableSpec.getColumnSpec(tableSpec.findColumnIndex(src)).getType();
                }
            } else {
                outType = tableSpec.getColumnSpec(tableSpec.findColumnIndex(src)).getType();
            }
            outputCols.add(new DataColumnSpecCreator(outName, outType).createSpec());
        }

        // 2. Unedited columns in original order
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = tableSpec.getColumnSpec(i);
            if (!editedOriginals.contains(colSpec.getName())) {
                outputCols.add(colSpec);
            }
        }

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    /**
     * Maps a Spark data type string to KNIME DataType.
     * Returns null for DATE/TIMESTAMP (spec determined by job).
     */
    private static DataType mapSparkTypeToKNIME(final String sparkType) {
        switch (sparkType) {
            case "STRING":  return StringCell.TYPE;
            case "INTEGER": return IntCell.TYPE;
            case "LONG":    return LongCell.TYPE;
            case "DOUBLE":  return DoubleCell.TYPE;
            case "FLOAT":   return DoubleCell.TYPE;  // Float maps to Double in KNIME
            case "BOOLEAN": return BooleanCell.TYPE;
            case "DATE":
            case "TIMESTAMP":
                // Configure-time uses original type; execute-time gets actual type from job output
                return null;
            default:
                return null;
        }
    }

    // ── Filter helper ─────────────────────────────────────────────────────

    /** Result of filtering settings arrays against the current input spec. */
    private static final class FilteredColumns {
        final String[] sourceColumns;
        final String[] newNames;
        final String[] newTypes;
        final List<String> missingColumns;

        FilteredColumns(final String[] sourceColumns, final String[] newNames,
                final String[] newTypes, final List<String> missingColumns) {
            this.sourceColumns = sourceColumns;
            this.newNames = newNames;
            this.newTypes = newTypes;
            this.missingColumns = missingColumns;
        }
    }

    /**
     * Filters settings arrays to include only columns present in the input spec.
     * Missing columns are collected for a warning message.
     */
    private static FilteredColumns filterExistingColumns(final String[] srcCols, final String[] names,
            final String[] types, final DataTableSpec tableSpec) {

        final List<String> filteredSrc = new ArrayList<>();
        final List<String> filteredNames = new ArrayList<>();
        final List<String> filteredTypes = new ArrayList<>();
        final List<String> missing = new ArrayList<>();

        for (int i = 0; i < srcCols.length; i++) {
            final String src = srcCols[i];
            if (src != null && tableSpec.findColumnIndex(src) >= 0) {
                filteredSrc.add(src);
                filteredNames.add(i < names.length ? names[i] : "");
                filteredTypes.add(i < types.length ? types[i] : "KEEP");
            } else if (src != null && !src.isEmpty()) {
                missing.add(src);
            }
        }

        return new FilteredColumns(
            filteredSrc.toArray(new String[0]),
            filteredNames.toArray(new String[0]),
            filteredTypes.toArray(new String[0]),
            missing);
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
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }
}
