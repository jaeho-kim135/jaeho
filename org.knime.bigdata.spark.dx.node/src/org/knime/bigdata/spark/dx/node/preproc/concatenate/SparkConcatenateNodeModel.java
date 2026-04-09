package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import org.knime.core.data.DataType;
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
 * Node model for the Spark Concatenate node.
 * Vertically concatenates two Spark DataFrames with configurable column mapping.
 * Output column order follows the mapping array order (= config table order).
 * Duplicate left column names get "(1)", "(2)" suffixes via makeUniqueName().
 */
public class SparkConcatenateNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkConcatenateNodeModel.class.getCanonicalName();

    private final SparkConcatenateSettings m_settings = new SparkConcatenateSettings();

    /** Constructor with 2 input ports and 1 output port. */
    public SparkConcatenateNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Both left and right Spark DataFrame inputs are required.");
        }

        final SparkDataPortObjectSpec leftSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final SparkDataPortObjectSpec rightSpec = (SparkDataPortObjectSpec) inSpecs[1];
        final DataTableSpec leftTableSpec = leftSpec.getTableSpec();
        final DataTableSpec rightTableSpec = rightSpec.getTableSpec();

        // Auto-map when node is not yet configured
        String[] leftCols = m_settings.getLeftColumns();
        String[] rightCols = m_settings.getRightColumns();
        if (!m_settings.isNodeConfigured() && leftCols.length == 0) {
            final String[][] autoMap = buildAutoMapping(leftTableSpec, rightTableSpec);
            leftCols = autoMap[0];
            rightCols = autoMap[1];
        }

        // Filter out rows referencing missing columns (warn instead of error)
        final String[][] filtered = filterMissingColumns(leftCols, rightCols, leftTableSpec, rightTableSpec);
        leftCols = filtered[0];
        rightCols = filtered[1];

        final String unmatchedLeft = m_settings.getUnmatchedLeftAction();
        final String unmatchedRight = m_settings.getUnmatchedRightAction();

        if (leftCols.length == 0 && "EXCLUDE".equals(unmatchedLeft) && "EXCLUDE".equals(unmatchedRight)) {
            setWarningMessage("No column mappings and both unmatched actions are Exclude. "
                + "Output will have zero columns.");
        }

        // Build output spec following array order (may return null for date promotion cases)
        final DataTableSpec outputSpec = buildOutputSpec(
            leftCols, rightCols, unmatchedLeft, unmatchedRight, leftTableSpec, rightTableSpec);
        if (outputSpec == null) {
            return new PortObjectSpec[]{null};
        }

        return new PortObjectSpec[]{new SparkDataPortObjectSpec(leftSpec.getContextID(), outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject leftPort = (SparkDataPortObject) inData[0];
        final SparkDataPortObject rightPort = (SparkDataPortObject) inData[1];
        final SparkContextID contextID = leftPort.getContextID();
        final String leftInput = leftPort.getData().getID();
        final String rightInput = rightPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        String[] execLeftCols = m_settings.getLeftColumns();
        String[] execRightCols = m_settings.getRightColumns();
        if (!m_settings.isNodeConfigured() && execLeftCols.length == 0) {
            final String[][] autoMap = buildAutoMapping(leftPort.getTableSpec(), rightPort.getTableSpec());
            execLeftCols = autoMap[0];
            execRightCols = autoMap[1];
        }

        // Filter out missing columns (warn instead of error)
        final String[][] filtered = filterMissingColumns(
            execLeftCols, execRightCols, leftPort.getTableSpec(), rightPort.getTableSpec());
        execLeftCols = filtered[0];
        execRightCols = filtered[1];

        final SparkConcatenateJobInput jobInput = new SparkConcatenateJobInput(
            leftInput, rightInput, outputObject,
            execLeftCols,
            execRightCols,
            m_settings.getUnmatchedLeftAction(),
            m_settings.getUnmatchedRightAction());

        exec.setMessage("Executing Spark concatenate job...");
        final SparkConcatenateJobOutput jobOutput = SparkContextUtil
            .<SparkConcatenateJobInput, SparkConcatenateJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    // ── Auto-mapping ──────────────────────────────────────────────────────────

    /**
     * Builds auto-mapping arrays: all left columns in order (same-named auto-mapped),
     * then right-only columns appended with empty left cell.
     * Matches the Vue autoMap() behavior exactly.
     * @return String[2] where [0] = leftColumns, [1] = rightColumns
     */
    private static String[][] buildAutoMapping(final DataTableSpec leftSpec, final DataTableSpec rightSpec) {
        final List<String> autoLeft = new ArrayList<>();
        final List<String> autoRight = new ArrayList<>();
        final Set<String> usedRight = new HashSet<>();

        // All left columns in order; auto-map if same name exists in right
        for (int i = 0; i < leftSpec.getNumColumns(); i++) {
            final String colName = leftSpec.getColumnSpec(i).getName();
            autoLeft.add(colName);
            if (rightSpec.containsName(colName)) {
                autoRight.add(colName);
                usedRight.add(colName);
            } else {
                autoRight.add("");
            }
        }

        // Right-only columns appended (empty left cell)
        for (int i = 0; i < rightSpec.getNumColumns(); i++) {
            final String colName = rightSpec.getColumnSpec(i).getName();
            if (!usedRight.contains(colName)) {
                autoLeft.add("");
                autoRight.add(colName);
            }
        }

        return new String[][]{autoLeft.toArray(new String[0]), autoRight.toArray(new String[0])};
    }

    // ── Missing column handling ───────────────────────────────────────────────

    /**
     * Filters out mapping rows that reference columns no longer present in the input specs.
     * Sets a warning message listing the removed columns instead of throwing an error.
     */
    private String[][] filterMissingColumns(final String[] leftCols, final String[] rightCols,
            final DataTableSpec leftSpec, final DataTableSpec rightSpec) {

        final List<String> filteredLeft = new ArrayList<>();
        final List<String> filteredRight = new ArrayList<>();
        final List<String> missingCols = new ArrayList<>();
        final int len = Math.max(leftCols.length, rightCols.length);

        for (int i = 0; i < len; i++) {
            final String left = (i < leftCols.length) ? leftCols[i] : "";
            final String right = (i < rightCols.length) ? rightCols[i] : "";
            final boolean hasLeft = left != null && !left.isEmpty();
            final boolean hasRight = right != null && !right.isEmpty();

            final boolean leftMissing = hasLeft && leftSpec.findColumnIndex(left) == -1;
            final boolean rightMissing = hasRight && rightSpec.findColumnIndex(right) == -1;

            if (leftMissing || rightMissing) {
                // Skip this row — column(s) no longer exist
                if (leftMissing) {
                    missingCols.add("Left:'" + left + "'");
                }
                if (rightMissing) {
                    missingCols.add("Right:'" + right + "'");
                }
                continue;
            }

            filteredLeft.add(left != null ? left : "");
            filteredRight.add(right != null ? right : "");
        }

        if (!missingCols.isEmpty()) {
            setWarningMessage("Skipped mappings with missing columns: " + String.join(", ", missingCols));
        }

        return new String[][]{filteredLeft.toArray(new String[0]), filteredRight.toArray(new String[0])};
    }

    // ── Output spec building (follows array order) ────────────────────────────

    /**
     * Builds the output DataTableSpec following mapping array order.
     * Each (left[i], right[i]) pair becomes one output column.
     * Duplicate output names get "(1)", "(2)" suffixes via makeUniqueName().
     */
    private DataTableSpec buildOutputSpec(final String[] leftCols, final String[] rightCols,
            final String unmatchedLeft, final String unmatchedRight,
            final DataTableSpec leftSpec, final DataTableSpec rightSpec) {

        final List<DataColumnSpec> outputCols = new ArrayList<>();
        final Set<String> usedNames = new LinkedHashSet<>();

        for (int i = 0; i < leftCols.length; i++) {
            final String left = leftCols[i];
            final String right = (i < rightCols.length) ? rightCols[i] : "";
            final boolean hasLeft = left != null && !left.isEmpty();
            final boolean hasRight = right != null && !right.isEmpty();

            if (hasLeft && hasRight) {
                // Mapped pair — resolve type
                final String outputName = makeUniqueName(left, usedNames);
                final DataType leftType = leftSpec.getColumnSpec(leftSpec.findColumnIndex(left)).getType();
                final DataType rightType = rightSpec.getColumnSpec(rightSpec.findColumnIndex(right)).getType();
                final DataType resolvedType = resolveTypePairKNIME(leftType, rightType);
                if (resolvedType == null) {
                    return null; // defer to job (e.g. date promotion)
                }
                outputCols.add(new DataColumnSpecCreator(outputName, resolvedType).createSpec());
                usedNames.add(outputName);
            } else if (hasLeft) {
                if ("FILL_NULL".equals(unmatchedLeft)) {
                    final String outputName = makeUniqueName(left, usedNames);
                    final DataType leftType = leftSpec.getColumnSpec(leftSpec.findColumnIndex(left)).getType();
                    outputCols.add(new DataColumnSpecCreator(outputName, leftType).createSpec());
                    usedNames.add(outputName);
                }
            } else if (hasRight) {
                if ("FILL_NULL".equals(unmatchedRight)) {
                    final String outputName = makeUniqueName(right, usedNames);
                    final DataType rightType = rightSpec.getColumnSpec(rightSpec.findColumnIndex(right)).getType();
                    outputCols.add(new DataColumnSpecCreator(outputName, rightType).createSpec());
                    usedNames.add(outputName);
                }
            }
        }

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    // ── Type resolution (pair-based) ──────────────────────────────────────────

    /**
     * Resolves output type for a single mapped pair (KNIME types).
     * Same type → keep; both numeric → promote; either String → String;
     * different non-numeric non-string (e.g. Date vs Timestamp) → null (defer to job).
     */
    private DataType resolveTypePairKNIME(final DataType leftType, final DataType rightType) {
        if (leftType.equals(rightType)) {
            return leftType;
        }
        // Both numeric → promote
        if (isNumericKNIME(leftType) && isNumericKNIME(rightType)) {
            if (leftType.equals(DoubleCell.TYPE) || rightType.equals(DoubleCell.TYPE)) {
                return DoubleCell.TYPE;
            }
            if (leftType.equals(LongCell.TYPE) || rightType.equals(LongCell.TYPE)) {
                return LongCell.TYPE;
            }
            return IntCell.TYPE;
        }
        // Either is String → String
        if (leftType.equals(StringCell.TYPE) || rightType.equals(StringCell.TYPE)) {
            return StringCell.TYPE;
        }
        // Numeric + non-numeric mismatch → String
        if (isNumericKNIME(leftType) || isNumericKNIME(rightType)) {
            return StringCell.TYPE;
        }
        // Both non-numeric, non-string, but different (e.g. Date vs Timestamp) → defer to job
        return null;
    }

    // ── Utility methods ───────────────────────────────────────────────────────

    private String makeUniqueName(final String name, final Set<String> usedNames) {
        if (!usedNames.contains(name)) {
            return name;
        }
        int suffix = 1;
        while (usedNames.contains(name + "(" + suffix + ")")) {
            suffix++;
        }
        return name + "(" + suffix + ")";
    }

    private boolean isNumericKNIME(final DataType type) {
        return type.equals(IntCell.TYPE) || type.equals(LongCell.TYPE) || type.equals(DoubleCell.TYPE);
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
