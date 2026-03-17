package org.knime.bigdata.spark.dx.node.convert.datetimetostring;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Date&amp;Time to String node.
 *
 * <p>Stores selected columns in NameFilterConfiguration format (for compatibility with
 * LegacyColumnFilterPersistor), date format pattern, locale, append/replace mode, and
 * column suffix.</p>
 */
public final class SparkDateTimeToStringSettings {

    // ── Config keys ──────────────────────────────────────────────────────────
    static final String CFG_COLUMNS = "columns";
    static final String CFG_FORMAT = "format";
    static final String CFG_LOCALE = "locale";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_SUFFIX = "suffix";
    static final String CFG_CONFIGURED = "nodeConfigured";

    /** NameFilterConfiguration keys for column filter. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    // ── Fields ────────────────────────────────────────────────────────────────
    private final SettingsModelFilterString m_columns =
        new SettingsModelFilterString(CFG_COLUMNS, new String[0], new String[0], false);

    private final SettingsModelString m_format =
        new SettingsModelString(CFG_FORMAT, "yyyy-MM-dd HH:mm:ss");

    private final SettingsModelString m_locale =
        new SettingsModelString(CFG_LOCALE, "en");

    private final SettingsModelString m_appendOrReplace =
        new SettingsModelString(CFG_APPEND_OR_REPLACE, "REPLACE");

    private final SettingsModelString m_suffix =
        new SettingsModelString(CFG_SUFFIX, " (String)");

    private boolean m_nodeConfigured = false;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the selected column names */
    public List<String> getColumns() {
        return m_columns.getIncludeList();
    }

    /** @return the date format pattern */
    public String getFormat() {
        return m_format.getStringValue();
    }

    /** @return the locale string */
    public String getLocale() {
        return m_locale.getStringValue();
    }

    /** @return the append/replace mode string */
    public String getAppendOrReplace() {
        return m_appendOrReplace.getStringValue();
    }

    /** @return true if the output mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(m_appendOrReplace.getStringValue());
    }

    /** @return the column suffix for APPEND mode */
    public String getSuffix() {
        return m_suffix.getStringValue();
    }

    /** @return true if the node has been configured */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    // ── Save / Validate / Load ───────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_COLUMNS, m_columns.getIncludeList());
        m_format.saveSettingsTo(settings);
        m_locale.saveSettingsTo(settings);
        m_appendOrReplace.saveSettingsTo(settings);
        m_suffix.saveSettingsTo(settings);
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_format.validateSettings(settings);
        m_appendOrReplace.validateSettings(settings);
        m_suffix.validateSettings(settings);
        if (settings.containsKey(CFG_LOCALE)) {
            m_locale.validateSettings(settings);
        }
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_COLUMNS, m_columns);
        m_format.loadSettingsFrom(settings);
        m_appendOrReplace.loadSettingsFrom(settings);
        m_suffix.loadSettingsFrom(settings);
        if (settings.containsKey(CFG_LOCALE)) {
            m_locale.loadSettingsFrom(settings);
        }
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    // ── Column filter helpers ────────────────────────────────────────────────

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
     * Loads a column filter, handling both NameFilterConfiguration and
     * SettingsModelFilterString formats.
     */
    private static void loadColumnFilter(final NodeSettingsRO settings, final String key,
            final SettingsModelFilterString model) throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            return;
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                final String[] incl = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                model.setIncludeList(Arrays.asList(incl));
                return;
            }
        } catch (final InvalidSettingsException e) {
            // fall through
        }
        model.loadSettingsFrom(settings);
    }
}
