package org.knime.bigdata.spark.dx.node.preproc.stringtonumber;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark String to Number node.
 *
 * <p>Column filter settings are stored in NameFilterConfiguration format
 * (filter-type / included_names / excluded_names / enforce_option), which is the format
 * used by the WebUI dialog's LegacyColumnFilterPersistor.</p>
 */
public final class SparkStringToNumberSettings {

    static final String CFG_INCLUDE = "include";
    static final String CFG_PARSE_TYPE = "parse_type";
    static final String CFG_DECIMAL_SEPARATOR = "decimal_separator";
    static final String CFG_THOUSANDS_SEPARATOR = "thousands_separator";
    static final String CFG_GENERIC_PARSE = "generic_parse";
    static final String CFG_FAIL_ON_ERROR = "fail_on_error";

    /** Key used by NameFilterConfiguration for the filter type. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private String[] m_includedColumns = new String[0];
    private String m_parseType = "DOUBLE";
    private String m_decimalSeparator = ".";
    private String m_thousandsSeparator = "";
    private boolean m_genericParse = false;
    private boolean m_failOnError = false;

    /** @return the included column names */
    public List<String> getIncludedColumns() {
        return Arrays.asList(m_includedColumns);
    }

    /** @return the parse type (INTEGER, DOUBLE, LONG) */
    public String getParseType() {
        return m_parseType;
    }

    /** @return the decimal separator character (empty = default ".") */
    public String getDecimalSeparator() {
        return m_decimalSeparator;
    }

    /** @return the thousands separator character (empty = disabled) */
    public String getThousandsSeparator() {
        return m_thousandsSeparator;
    }

    /** @return whether generic parsing (d/D/f/F suffix) is enabled */
    public boolean isGenericParse() {
        return m_genericParse;
    }

    /** @return whether to fail on conversion error */
    public boolean isFailOnError() {
        return m_failOnError;
    }

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_INCLUDE, m_includedColumns);
        settings.addString(CFG_PARSE_TYPE, m_parseType);
        settings.addString(CFG_DECIMAL_SEPARATOR, m_decimalSeparator);
        settings.addString(CFG_THOUSANDS_SEPARATOR, m_thousandsSeparator);
        settings.addBoolean(CFG_GENERIC_PARSE, m_genericParse);
        settings.addBoolean(CFG_FAIL_ON_ERROR, m_failOnError);
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_INCLUDE)) {
            throw new InvalidSettingsException("Missing column filter configuration for key '" + CFG_INCLUDE + "'.");
        }
        // parse_type, decimal_separator, thousands_separator are required
        settings.getString(CFG_PARSE_TYPE);
        settings.getString(CFG_DECIMAL_SEPARATOR);
        settings.getString(CFG_THOUSANDS_SEPARATOR);
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_INCLUDE);
        m_parseType = settings.getString(CFG_PARSE_TYPE, "DOUBLE");
        m_decimalSeparator = settings.getString(CFG_DECIMAL_SEPARATOR, ".");
        m_thousandsSeparator = settings.getString(CFG_THOUSANDS_SEPARATOR, "");
        m_genericParse = settings.getBoolean(CFG_GENERIC_PARSE, false);
        m_failOnError = settings.getBoolean(CFG_FAIL_ON_ERROR, false);
    }

    // ── Column filter format helpers ──────────────────────────────────────────

    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final String[] included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, included);
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);
    }

    private void loadColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            return;
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                m_includedColumns = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                return;
            }
            // Fallback: old format
            m_includedColumns = sub.getStringArray("InclList", new String[0]);
        } catch (final InvalidSettingsException e) {
            m_includedColumns = new String[0];
        }
    }
}
