package knime.bigdata.spark_dx.node.unpivot;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

/**
 * Settings container for Spark Unpivot node.
 *
 * Goal (minimal):
 *  - valueColumns: columns to unpivot (melt)
 *  - retainedColumns: columns to keep as-is
 *  - skipMissingCells: drop rows with missing cells in value columns (optional)
 *
 * Note:
 *  - We store column lists as string arrays for now.
 *  - Later you can replace this with ColumnFilter2 (wildcard/regex/type) model if desired.
 */
public final class SparkUnpivotSettings {

    // ---- keys
    private static final String KEY_VALUE_COLS = "valueColumns";
    private static final String KEY_RETAINED_COLS = "retainedColumns";
    private static final String KEY_SKIP_MISSING = "skipMissingCells";

    // ---- models
    private final SettingsModelStringArray m_valueColumns =
        new SettingsModelStringArray(KEY_VALUE_COLS, new String[0]);

    private final SettingsModelStringArray m_retainedColumns =
        new SettingsModelStringArray(KEY_RETAINED_COLS, new String[0]);

    private final SettingsModelBoolean m_skipMissingCells =
        new SettingsModelBoolean(KEY_SKIP_MISSING, false);

    // ---- accessors
    public SettingsModelStringArray valueColumnsModel() {
        return m_valueColumns;
    }

    public SettingsModelStringArray retainedColumnsModel() {
        return m_retainedColumns;
    }

    public SettingsModelBoolean skipMissingCellsModel() {
        return m_skipMissingCells;
    }

    public String[] getValueColumns() {
        return m_valueColumns.getStringArrayValue();
    }

    public String[] getRetainedColumns() {
        return m_retainedColumns.getStringArrayValue();
    }

    public boolean isSkipMissingCells() {
        return m_skipMissingCells.getBooleanValue();
    }

    // ---- persistence
    public void saveTo(final NodeSettingsWO settings) {
        m_valueColumns.saveSettingsTo(settings);
        m_retainedColumns.saveSettingsTo(settings);
        m_skipMissingCells.saveSettingsTo(settings);
    }

    public void loadFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_valueColumns.loadSettingsFrom(settings);
        m_retainedColumns.loadSettingsFrom(settings);
        m_skipMissingCells.loadSettingsFrom(settings);
    }

    public void validate(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Validate structure only (no spec-based validation here)
        final SettingsModelStringArray tmpValue = new SettingsModelStringArray(KEY_VALUE_COLS, new String[0]);
        final SettingsModelStringArray tmpRetained = new SettingsModelStringArray(KEY_RETAINED_COLS, new String[0]);
        final SettingsModelBoolean tmpSkip = new SettingsModelBoolean(KEY_SKIP_MISSING, false);

        tmpValue.loadSettingsFrom(settings);
        tmpRetained.loadSettingsFrom(settings);
        tmpSkip.loadSettingsFrom(settings);

        if (tmpValue.getStringArrayValue() == null || tmpValue.getStringArrayValue().length == 0) {
            throw new InvalidSettingsException("Select at least one value column to unpivot.");
        }
    }
}
