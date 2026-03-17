package org.knime.bigdata.spark.dx.node.extract.datetimefields;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Extract Date&amp;Time Fields node.
 * Stores which fields to extract, the source column, locale, and output prefix.
 */
public final class SparkExtractDateTimeFieldsSettings {

    // ── Config Keys ──────────────────────────────────────────────────────────

    static final String CFG_COLUMN = "column";
    static final String CFG_EXTRACT_YEAR = "extractYear";
    static final String CFG_EXTRACT_MONTH = "extractMonth";
    static final String CFG_EXTRACT_DAY = "extractDay";
    static final String CFG_EXTRACT_DAY_OF_WEEK = "extractDayOfWeek";
    static final String CFG_EXTRACT_DAY_OF_YEAR = "extractDayOfYear";
    static final String CFG_EXTRACT_WEEK_OF_YEAR = "extractWeekOfYear";
    static final String CFG_EXTRACT_QUARTER = "extractQuarter";
    static final String CFG_EXTRACT_HOUR = "extractHour";
    static final String CFG_EXTRACT_MINUTE = "extractMinute";
    static final String CFG_EXTRACT_SECOND = "extractSecond";
    static final String CFG_EXTRACT_SUBSECOND = "extractSubsecond";
    static final String CFG_SUBSECOND_UNIT = "subsecondUnit";
    static final String CFG_EXTRACT_DAY_OF_WEEK_NAME = "extractDayOfWeekName";
    static final String CFG_EXTRACT_MONTH_NAME = "extractMonthName";
    static final String CFG_LOCALE = "locale";
    static final String CFG_COLUMN_PREFIX = "columnPrefix";

    // ── Fields ───────────────────────────────────────────────────────────────

    private String m_column = "";
    private boolean m_extractYear = false;
    private boolean m_extractMonth = false;
    private boolean m_extractDay = false;
    private boolean m_extractDayOfWeek = false;
    private boolean m_extractDayOfYear = false;
    private boolean m_extractWeekOfYear = false;
    private boolean m_extractQuarter = false;
    private boolean m_extractHour = false;
    private boolean m_extractMinute = false;
    private boolean m_extractSecond = false;
    private boolean m_extractSubsecond = false;
    private String m_subsecondUnit = "MILLISECOND";
    private boolean m_extractDayOfWeekName = false;
    private boolean m_extractMonthName = false;
    private String m_locale = "en";
    private String m_columnPrefix = "";

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the source date/time column name */
    public String getColumn() { return m_column; }

    /** @return true if Year should be extracted */
    public boolean isExtractYear() { return m_extractYear; }

    /** @return true if Month (number) should be extracted */
    public boolean isExtractMonth() { return m_extractMonth; }

    /** @return true if Day of month should be extracted */
    public boolean isExtractDay() { return m_extractDay; }

    /** @return true if Day of week (number) should be extracted */
    public boolean isExtractDayOfWeek() { return m_extractDayOfWeek; }

    /** @return true if Day of year should be extracted */
    public boolean isExtractDayOfYear() { return m_extractDayOfYear; }

    /** @return true if Week of year should be extracted */
    public boolean isExtractWeekOfYear() { return m_extractWeekOfYear; }

    /** @return true if Quarter should be extracted */
    public boolean isExtractQuarter() { return m_extractQuarter; }

    /** @return true if Hour should be extracted */
    public boolean isExtractHour() { return m_extractHour; }

    /** @return true if Minute should be extracted */
    public boolean isExtractMinute() { return m_extractMinute; }

    /** @return true if Second should be extracted */
    public boolean isExtractSecond() { return m_extractSecond; }

    /** @return true if Subsecond field should be extracted */
    public boolean isExtractSubsecond() { return m_extractSubsecond; }

    /** @return the subsecond unit (MILLISECOND, MICROSECOND, NANOSECOND) */
    public String getSubsecondUnit() { return m_subsecondUnit; }

    /** @return true if Day of week name should be extracted */
    public boolean isExtractDayOfWeekName() { return m_extractDayOfWeekName; }

    /** @return true if Month name should be extracted */
    public boolean isExtractMonthName() { return m_extractMonthName; }

    /** @return the locale string for name fields (e.g., "en", "ko") */
    public String getLocale() { return m_locale; }

    /** @return the column name prefix (empty string means no prefix) */
    public String getColumnPrefix() { return m_columnPrefix; }

    /**
     * Returns true if at least one field is selected for extraction.
     *
     * @return true if any extraction checkbox is checked
     */
    public boolean isAnyFieldSelected() {
        return m_extractYear || m_extractMonth || m_extractDay
            || m_extractDayOfWeek || m_extractDayOfYear || m_extractWeekOfYear
            || m_extractQuarter || m_extractHour || m_extractMinute
            || m_extractSecond || m_extractSubsecond
            || m_extractDayOfWeekName || m_extractMonthName;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_COLUMN, m_column);
        settings.addBoolean(CFG_EXTRACT_YEAR, m_extractYear);
        settings.addBoolean(CFG_EXTRACT_MONTH, m_extractMonth);
        settings.addBoolean(CFG_EXTRACT_DAY, m_extractDay);
        settings.addBoolean(CFG_EXTRACT_DAY_OF_WEEK, m_extractDayOfWeek);
        settings.addBoolean(CFG_EXTRACT_DAY_OF_YEAR, m_extractDayOfYear);
        settings.addBoolean(CFG_EXTRACT_WEEK_OF_YEAR, m_extractWeekOfYear);
        settings.addBoolean(CFG_EXTRACT_QUARTER, m_extractQuarter);
        settings.addBoolean(CFG_EXTRACT_HOUR, m_extractHour);
        settings.addBoolean(CFG_EXTRACT_MINUTE, m_extractMinute);
        settings.addBoolean(CFG_EXTRACT_SECOND, m_extractSecond);
        settings.addBoolean(CFG_EXTRACT_SUBSECOND, m_extractSubsecond);
        settings.addString(CFG_SUBSECOND_UNIT, m_subsecondUnit);
        settings.addBoolean(CFG_EXTRACT_DAY_OF_WEEK_NAME, m_extractDayOfWeekName);
        settings.addBoolean(CFG_EXTRACT_MONTH_NAME, m_extractMonthName);
        settings.addString(CFG_LOCALE, m_locale);
        settings.addString(CFG_COLUMN_PREFIX, m_columnPrefix);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_COLUMN);
        settings.getBoolean(CFG_EXTRACT_YEAR);
        settings.getBoolean(CFG_EXTRACT_MONTH);
        settings.getBoolean(CFG_EXTRACT_DAY);
        settings.getBoolean(CFG_EXTRACT_HOUR);
        settings.getBoolean(CFG_EXTRACT_MINUTE);
        settings.getBoolean(CFG_EXTRACT_SECOND);
        // Optional fields with backward-compatible defaults
        if (settings.containsKey(CFG_EXTRACT_DAY_OF_WEEK)) {
            settings.getBoolean(CFG_EXTRACT_DAY_OF_WEEK);
        }
        if (settings.containsKey(CFG_EXTRACT_DAY_OF_YEAR)) {
            settings.getBoolean(CFG_EXTRACT_DAY_OF_YEAR);
        }
        if (settings.containsKey(CFG_EXTRACT_WEEK_OF_YEAR)) {
            settings.getBoolean(CFG_EXTRACT_WEEK_OF_YEAR);
        }
        if (settings.containsKey(CFG_EXTRACT_QUARTER)) {
            settings.getBoolean(CFG_EXTRACT_QUARTER);
        }
        if (settings.containsKey(CFG_EXTRACT_SUBSECOND)) {
            settings.getBoolean(CFG_EXTRACT_SUBSECOND);
        }
        if (settings.containsKey(CFG_SUBSECOND_UNIT)) {
            settings.getString(CFG_SUBSECOND_UNIT);
        }
        if (settings.containsKey(CFG_EXTRACT_DAY_OF_WEEK_NAME)) {
            settings.getBoolean(CFG_EXTRACT_DAY_OF_WEEK_NAME);
        }
        if (settings.containsKey(CFG_EXTRACT_MONTH_NAME)) {
            settings.getBoolean(CFG_EXTRACT_MONTH_NAME);
        }
        if (settings.containsKey(CFG_LOCALE)) {
            settings.getString(CFG_LOCALE);
        }
        if (settings.containsKey(CFG_COLUMN_PREFIX)) {
            settings.getString(CFG_COLUMN_PREFIX);
        }
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column = settings.getString(CFG_COLUMN);
        m_extractYear = settings.getBoolean(CFG_EXTRACT_YEAR);
        m_extractMonth = settings.getBoolean(CFG_EXTRACT_MONTH);
        m_extractDay = settings.getBoolean(CFG_EXTRACT_DAY);
        m_extractHour = settings.getBoolean(CFG_EXTRACT_HOUR);
        m_extractMinute = settings.getBoolean(CFG_EXTRACT_MINUTE);
        m_extractSecond = settings.getBoolean(CFG_EXTRACT_SECOND);
        // Optional fields with backward-compatible defaults
        m_extractDayOfWeek = settings.containsKey(CFG_EXTRACT_DAY_OF_WEEK)
            ? settings.getBoolean(CFG_EXTRACT_DAY_OF_WEEK) : false;
        m_extractDayOfYear = settings.containsKey(CFG_EXTRACT_DAY_OF_YEAR)
            ? settings.getBoolean(CFG_EXTRACT_DAY_OF_YEAR) : false;
        m_extractWeekOfYear = settings.containsKey(CFG_EXTRACT_WEEK_OF_YEAR)
            ? settings.getBoolean(CFG_EXTRACT_WEEK_OF_YEAR) : false;
        m_extractQuarter = settings.containsKey(CFG_EXTRACT_QUARTER)
            ? settings.getBoolean(CFG_EXTRACT_QUARTER) : false;
        m_extractSubsecond = settings.containsKey(CFG_EXTRACT_SUBSECOND)
            ? settings.getBoolean(CFG_EXTRACT_SUBSECOND) : false;
        m_subsecondUnit = settings.containsKey(CFG_SUBSECOND_UNIT)
            ? settings.getString(CFG_SUBSECOND_UNIT) : "MILLISECOND";
        m_extractDayOfWeekName = settings.containsKey(CFG_EXTRACT_DAY_OF_WEEK_NAME)
            ? settings.getBoolean(CFG_EXTRACT_DAY_OF_WEEK_NAME) : false;
        m_extractMonthName = settings.containsKey(CFG_EXTRACT_MONTH_NAME)
            ? settings.getBoolean(CFG_EXTRACT_MONTH_NAME) : false;
        m_locale = settings.containsKey(CFG_LOCALE)
            ? settings.getString(CFG_LOCALE) : "en";
        m_columnPrefix = settings.containsKey(CFG_COLUMN_PREFIX)
            ? settings.getString(CFG_COLUMN_PREFIX) : "";
    }
}
