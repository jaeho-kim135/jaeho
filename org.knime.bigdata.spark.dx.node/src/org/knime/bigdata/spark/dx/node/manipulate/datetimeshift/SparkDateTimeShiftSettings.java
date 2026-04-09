package org.knime.bigdata.spark.dx.node.manipulate.datetimeshift;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Date&amp;Time Shift node.
 * Stores selected columns, shift mode, shift value/column, granularity,
 * append/replace mode, and suffix.
 */
public final class SparkDateTimeShiftSettings {

    static final String CFG_COLUMNS = "columns";
    static final String CFG_SHIFT_MODE = "shiftMode";
    static final String CFG_SHIFT_VALUE = "shiftValue";
    static final String CFG_SHIFT_COLUMN = "shiftColumn";
    static final String CFG_GRANULARITY = "granularity";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_SUFFIX = "suffix";
    static final String CFG_CONFIGURED = "nodeConfigured";

    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";

    private List<String> m_columns = Collections.emptyList();
    private String m_shiftMode = "FIXED";
    private int m_shiftValue = 1;
    private String m_shiftColumn = "";
    private String m_granularity = "DAY";
    private String m_appendOrReplace = "REPLACE";
    private String m_suffix = "_shifted";
    private boolean m_nodeConfigured = false;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the list of selected date/time column names */
    public List<String> getColumns() {
        return m_columns;
    }

    /** @return the shift mode (FIXED or COLUMN) */
    public String getShiftMode() {
        return m_shiftMode;
    }

    /** @return the fixed shift value */
    public int getShiftValue() {
        return m_shiftValue;
    }

    /** @return the shift column name (for COLUMN mode) */
    public String getShiftColumn() {
        return m_shiftColumn;
    }

    /** @return the granularity (YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND) */
    public String getGranularity() {
        return m_granularity;
    }

    /** @return the output mode (REPLACE or APPEND) */
    public String getAppendOrReplace() {
        return m_appendOrReplace;
    }

    /** @return true if the output mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(m_appendOrReplace);
    }

    /** @return the suffix for appended columns */
    public String getSuffix() {
        return m_suffix;
    }

    /** @return true if the node has been configured at least once */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_COLUMNS, m_columns);
        settings.addString(CFG_SHIFT_MODE, m_shiftMode);
        settings.addInt(CFG_SHIFT_VALUE, m_shiftValue);
        settings.addString(CFG_SHIFT_COLUMN, m_shiftColumn);
        settings.addString(CFG_GRANULARITY, m_granularity);
        settings.addString(CFG_APPEND_OR_REPLACE, m_appendOrReplace);
        settings.addString(CFG_SUFFIX, m_suffix);
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateColumnFilter(settings, CFG_COLUMNS);
        settings.getString(CFG_SHIFT_MODE);
        // shiftValue uses containsKey in loadSettingsFrom for backward compatibility
        if (settings.containsKey(CFG_SHIFT_VALUE)) {
            settings.getInt(CFG_SHIFT_VALUE);
        }
        settings.getString(CFG_SHIFT_COLUMN);
        settings.getString(CFG_GRANULARITY);
        settings.getString(CFG_APPEND_OR_REPLACE);
        settings.getString(CFG_SUFFIX);
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_columns = loadColumnFilterIncluded(settings, CFG_COLUMNS);
        m_shiftMode = settings.getString(CFG_SHIFT_MODE, "FIXED");
        m_shiftValue = settings.containsKey(CFG_SHIFT_VALUE) ? settings.getInt(CFG_SHIFT_VALUE) : 1;
        m_shiftColumn = settings.getString(CFG_SHIFT_COLUMN, "");
        m_granularity = settings.getString(CFG_GRANULARITY, "DAY");
        m_appendOrReplace = settings.getString(CFG_APPEND_OR_REPLACE, "REPLACE");
        m_suffix = settings.getString(CFG_SUFFIX, "_shifted");
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    // ── Column filter helpers ────────────────────────────────────────────────

    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final List<String> included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, "STANDARD");
        sub.addStringArray(KEY_INCLUDED_NAMES, included.toArray(new String[0]));
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, "EnforceInclusion");
    }

    private static List<String> loadColumnFilterIncluded(final NodeSettingsRO settings, final String key) {
        if (!settings.containsKey(key)) {
            return Collections.emptyList();
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                return Arrays.asList(sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]));
            }
            if (sub.containsKey("InclList")) {
                return Arrays.asList(sub.getStringArray("InclList", new String[0]));
            }
        } catch (final InvalidSettingsException e) {
            // ignore
        }
        return Collections.emptyList();
    }

    private static void validateColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            throw new InvalidSettingsException("Missing column filter for key '" + key + "'.");
        }
    }
}
