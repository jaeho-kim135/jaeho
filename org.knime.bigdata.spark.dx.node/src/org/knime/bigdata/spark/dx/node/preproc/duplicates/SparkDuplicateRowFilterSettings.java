package org.knime.bigdata.spark.dx.node.preproc.duplicates;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Duplicate Row Filter node.
 *
 * <p>Column filter settings are stored in NameFilterConfiguration format
 * (filter-type / included_names / excluded_names / enforce_option), which is the format
 * used by the WebUI dialog's LegacyColumnFilterPersistor. For backward compatibility,
 * loadSettingsFrom() also accepts the old SettingsModelFilterString format
 * (InclList / ExclList / keep_all_columns_selected).</p>
 */
public final class SparkDuplicateRowFilterSettings {

    static final String CFG_COLUMNS = "columns";
    static final String CFG_DUPLICATE_HANDLING = "duplicateHandling";
    static final String CFG_ROW_SELECTION = "rowSelection";
    static final String CFG_ORDER_COLUMN = "orderColumn";
    static final String CFG_ORDER_DIRECTION = "orderDirection";
    static final String CFG_ADD_STATUS_COLUMN = "addStatusColumn";
    static final String CFG_STATUS_COLUMN_NAME = "statusColumnName";
    static final String CFG_CONFIGURED = "nodeConfigured";

    /** Key used by NameFilterConfiguration for the filter type. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    /** Filter type value for manual (name-based) selection. */
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    /** Key used by NameFilterConfiguration for the included names list. */
    private static final String KEY_INCLUDED_NAMES = "included_names";
    /** Key used by NameFilterConfiguration for the excluded names list. */
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    /** Key used by NameFilterConfiguration for the enforce option. */
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    /** EnforceOption value meaning "include list is authoritative". */
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private final SettingsModelFilterString m_columns =
        new SettingsModelFilterString(CFG_COLUMNS, new String[0], new String[0], false);

    private final SettingsModelString m_duplicateHandling =
        new SettingsModelString(CFG_DUPLICATE_HANDLING, "REMOVE");

    private final SettingsModelString m_rowSelection =
        new SettingsModelString(CFG_ROW_SELECTION, "FIRST");

    private final SettingsModelString m_orderColumn =
        new SettingsModelString(CFG_ORDER_COLUMN, "");

    private final SettingsModelString m_orderDirection =
        new SettingsModelString(CFG_ORDER_DIRECTION, "ASC");

    private final SettingsModelBoolean m_addStatusColumn =
        new SettingsModelBoolean(CFG_ADD_STATUS_COLUMN, false);

    private final SettingsModelString m_statusColumnName =
        new SettingsModelString(CFG_STATUS_COLUMN_NAME, "Duplicate Status");

    /** True once the user has accepted the dialog settings with OK at least once. */
    private boolean m_nodeConfigured = false;

    // ── Getters ───────────────────────────────────────────────────────────────

    /** @return true if the node has been configured */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    /** @return list of duplicate detection column names */
    public List<String> getColumns() {
        return m_columns.getIncludeList();
    }

    /** @return the duplicate handling mode string (REMOVE/KEEP) */
    public String getDuplicateHandling() {
        return m_duplicateHandling.getStringValue();
    }

    /** @return the row selection mode string (FIRST/LAST/MINIMUM/MAXIMUM/REMOVE_ALL) */
    public String getRowSelection() {
        return m_rowSelection.getStringValue();
    }

    /** @return the order column name */
    public String getOrderColumn() {
        return m_orderColumn.getStringValue();
    }

    /** @return the order direction string (ASC/DESC) */
    public String getOrderDirection() {
        return m_orderDirection.getStringValue();
    }

    /** @return whether to add a status column */
    public boolean isAddStatusColumn() {
        return m_addStatusColumn.getBooleanValue();
    }

    /** @return the status column name */
    public String getStatusColumnName() {
        return m_statusColumnName.getStringValue();
    }

    /** @return true if order direction is ascending */
    public boolean isAscending() {
        return "ASC".equals(m_orderDirection.getStringValue());
    }

    // ── Save / Validate / Load ────────────────────────────────────────────────

    /**
     * Save settings. Column filters are written in NameFilterConfiguration format.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_COLUMNS, m_columns.getIncludeList());
        m_duplicateHandling.saveSettingsTo(settings);
        m_rowSelection.saveSettingsTo(settings);
        m_orderColumn.saveSettingsTo(settings);
        m_orderDirection.saveSettingsTo(settings);
        m_addStatusColumn.saveSettingsTo(settings);
        m_statusColumnName.saveSettingsTo(settings);
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateColumnFilter(settings, CFG_COLUMNS);
        m_duplicateHandling.validateSettings(settings);
        m_rowSelection.validateSettings(settings);
        m_orderColumn.validateSettings(settings);
        m_orderDirection.validateSettings(settings);
        m_addStatusColumn.validateSettings(settings);
        m_statusColumnName.validateSettings(settings);
    }

    /**
     * Load validated settings.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_COLUMNS, m_columns);
        m_duplicateHandling.loadSettingsFrom(settings);
        m_rowSelection.loadSettingsFrom(settings);
        m_orderColumn.loadSettingsFrom(settings);
        m_orderDirection.loadSettingsFrom(settings);
        m_addStatusColumn.loadSettingsFrom(settings);
        m_statusColumnName.loadSettingsFrom(settings);
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    // ── Column filter format helpers ──────────────────────────────────────────

    /**
     * Writes a column filter in NameFilterConfiguration format.
     */
    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final List<String> included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, included.toArray(new String[0]));
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);
    }

    /**
     * Loads a column filter, handling both formats:
     * <ul>
     *   <li>New (NameFilterConfiguration): sub-config contains {@code included_names}</li>
     *   <li>Old (SettingsModelFilterString): sub-config contains {@code InclList}</li>
     * </ul>
     */
    private static void loadColumnFilter(final NodeSettingsRO settings, final String key,
            final SettingsModelFilterString model) throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            return;
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                // New format: NameFilterConfiguration / LegacyColumnFilterPersistor
                final String[] incl = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                model.setIncludeList(Arrays.asList(incl));
                return;
            }
        } catch (final InvalidSettingsException e) {
            // Sub-config is Config type (old format) -- fall through
        }
        // Old format: SettingsModelFilterString (InclList / ExclList)
        model.loadSettingsFrom(settings);
    }

    /**
     * Validates a column filter entry. Accepts both formats.
     */
    private static void validateColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            throw new InvalidSettingsException(
                "Missing column filter configuration for key '" + key + "'.");
        }
    }
}
