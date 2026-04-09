package org.knime.bigdata.spark.dx.node.preproc.stringtodatetime;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark String to Date&Time node.
 */
public final class SparkStringToDateTimeSettings {

    static final String CFG_INCLUDE = "include";
    static final String CFG_FORMAT = "format";
    static final String CFG_OUTPUT_TYPE = "output_type";
    static final String CFG_LOCALE = "locale";
    static final String CFG_FAIL_ON_ERROR = "fail_on_error";

    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private String[] m_includedColumns = new String[0];
    private String m_format = "yyyy-MM-dd";
    private String m_outputType = "DATE";
    private String m_locale = Locale.getDefault().toLanguageTag();
    private boolean m_failOnError = false;

    /** @return the included column names */
    public List<String> getIncludedColumns() {
        return Arrays.asList(m_includedColumns);
    }

    /** @return the format pattern */
    public String getFormat() {
        return m_format;
    }

    /** @return the output type (DATE, TIME, DATE_TIME, ZONED_DATE_TIME) */
    public String getOutputType() {
        return m_outputType;
    }

    /** @return the locale language tag */
    public String getLocale() {
        return m_locale;
    }

    /** @return whether to fail on parsing errors */
    public boolean isFailOnError() {
        return m_failOnError;
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        final NodeSettingsWO sub = settings.addNodeSettings(CFG_INCLUDE);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, m_includedColumns);
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);

        settings.addString(CFG_FORMAT, m_format);
        settings.addString(CFG_OUTPUT_TYPE, m_outputType);
        settings.addString(CFG_LOCALE, m_locale);
        settings.addBoolean(CFG_FAIL_ON_ERROR, m_failOnError);
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_INCLUDE)) {
            throw new InvalidSettingsException("Missing column filter configuration.");
        }
        final String fmt = settings.getString(CFG_FORMAT, "");
        if (fmt.trim().isEmpty()) {
            throw new InvalidSettingsException("Date&Time format must not be empty.");
        }
        settings.getString(CFG_OUTPUT_TYPE);
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (settings.containsKey(CFG_INCLUDE)) {
            try {
                final NodeSettingsRO sub = settings.getNodeSettings(CFG_INCLUDE);
                if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                    m_includedColumns = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                } else {
                    m_includedColumns = sub.getStringArray("InclList", new String[0]);
                }
            } catch (final InvalidSettingsException e) {
                m_includedColumns = new String[0];
            }
        }
        m_format = settings.getString(CFG_FORMAT, "yyyy-MM-dd");
        m_outputType = settings.getString(CFG_OUTPUT_TYPE, "DATE");
        m_locale = settings.getString(CFG_LOCALE, Locale.getDefault().toLanguageTag());
        m_failOnError = settings.getBoolean(CFG_FAIL_ON_ERROR, false);
    }
}
