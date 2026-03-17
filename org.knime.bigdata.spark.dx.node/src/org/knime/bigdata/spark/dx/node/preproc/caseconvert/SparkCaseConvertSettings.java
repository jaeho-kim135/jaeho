package org.knime.bigdata.spark.dx.node.preproc.caseconvert;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Case Converter node.
 * Stores selected columns and case conversion mode.
 */
public final class SparkCaseConvertSettings {

    static final String CFG_COLUMNS = "columns";
    static final String CFG_MODE = "caseMode";
    static final String CFG_CONFIGURED = "nodeConfigured";

    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private final SettingsModelFilterString m_columns =
        new SettingsModelFilterString(CFG_COLUMNS, new String[0], new String[0], false);
    private final SettingsModelString m_mode =
        new SettingsModelString(CFG_MODE, "UPPERCASE");
    private boolean m_nodeConfigured = false;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the list of selected column names */
    public List<String> getColumns() {
        return m_columns.getIncludeList();
    }

    /** @return the case mode as string (UPPERCASE, LOWERCASE, TITLE_CASE) */
    public String getMode() {
        return m_mode.getStringValue();
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
        writeColumnFilter(settings, CFG_COLUMNS, m_columns.getIncludeList());
        m_mode.saveSettingsTo(settings);
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
        m_mode.validateSettings(settings);
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_COLUMNS, m_columns);
        m_mode.loadSettingsFrom(settings);
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    // ── Column filter helpers ────────────────────────────────────────────────

    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final List<String> included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, included.toArray(new String[0]));
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);
    }

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
            /* fall through */
        }
        model.loadSettingsFrom(settings);
    }

    private static void validateColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            throw new InvalidSettingsException("Missing column filter for key '" + key + "'.");
        }
    }
}
