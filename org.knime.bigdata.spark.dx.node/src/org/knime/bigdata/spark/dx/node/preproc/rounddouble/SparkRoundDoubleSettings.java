package org.knime.bigdata.spark.dx.node.preproc.rounddouble;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;

/**
 * Settings for the Spark Number Rounder node.
 *
 * <p>Stores column filter in NameFilterConfiguration format (for compatibility with
 * LegacyColumnFilterPersistor used by the WebUI dialog), plus scalar settings for
 * number mode, precision, rounding method, output mode, and suffix.</p>
 */
public final class SparkRoundDoubleSettings {

    // ── Config keys ──────────────────────────────────────────────────────────
    static final String CFG_COLUMNS = "columns";
    static final String CFG_NUMBER_MODE = "numberMode";
    static final String CFG_PRECISION = "precision";
    static final String CFG_ROUNDING_STANDARD = "roundingStandard";
    static final String CFG_ROUNDING_ADVANCED = "roundingAdvanced";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_SUFFIX = "suffix";

    /** NameFilterConfiguration keys. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    // ── Fields ────────────────────────────────────────────────────────────────
    private final SettingsModelFilterString m_columns =
        new SettingsModelFilterString(CFG_COLUMNS, new String[0], new String[0], false);

    private String m_numberMode = "DECIMALS";
    private int m_precision = 3;
    private String m_roundingStandard = "HALF_AWAY_FROM_ZERO";
    private String m_roundingAdvanced = "AWAY_FROM_ZERO";
    private String m_appendOrReplace = "REPLACE";
    private String m_suffix = " (Rounded)";

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the selected column names */
    public List<String> getColumns() {
        return m_columns.getIncludeList();
    }

    /** @return the number mode (DECIMALS, SIGNIFICANT_DIGITS, INTEGER) */
    public String getNumberMode() {
        return m_numberMode;
    }

    /** @return the precision (decimal places or significant digits) */
    public int getPrecision() {
        return m_precision;
    }

    /** @return the rounding standard (HALF_AWAY_FROM_ZERO or OTHER) */
    public String getRoundingStandard() {
        return m_roundingStandard;
    }

    /** @return the advanced rounding method */
    public String getRoundingAdvanced() {
        return m_roundingAdvanced;
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

    // ── Save / Validate / Load ───────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_COLUMNS, m_columns.getIncludeList());
        settings.addString(CFG_NUMBER_MODE, m_numberMode);
        settings.addInt(CFG_PRECISION, m_precision);
        settings.addString(CFG_ROUNDING_STANDARD, m_roundingStandard);
        settings.addString(CFG_ROUNDING_ADVANCED, m_roundingAdvanced);
        settings.addString(CFG_APPEND_OR_REPLACE, m_appendOrReplace);
        settings.addString(CFG_SUFFIX, m_suffix);
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_COLUMNS)) {
            throw new InvalidSettingsException("Missing column filter configuration.");
        }
        settings.getString(CFG_NUMBER_MODE);
        settings.getInt(CFG_PRECISION);
        settings.getString(CFG_ROUNDING_STANDARD);
        settings.getString(CFG_ROUNDING_ADVANCED);
        settings.getString(CFG_APPEND_OR_REPLACE);
        settings.getString(CFG_SUFFIX);
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_COLUMNS, m_columns);
        m_numberMode = settings.getString(CFG_NUMBER_MODE);
        m_precision = settings.getInt(CFG_PRECISION);
        m_roundingStandard = settings.getString(CFG_ROUNDING_STANDARD);
        m_roundingAdvanced = settings.getString(CFG_ROUNDING_ADVANCED);
        m_appendOrReplace = settings.getString(CFG_APPEND_OR_REPLACE);
        m_suffix = settings.getString(CFG_SUFFIX);
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
