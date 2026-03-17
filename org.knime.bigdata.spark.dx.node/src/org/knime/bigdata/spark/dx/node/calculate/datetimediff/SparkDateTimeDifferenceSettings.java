package org.knime.bigdata.spark.dx.node.calculate.datetimediff;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Date&Time Difference node.
 * Stores first column, second mode, second column / fixed value, direction, granularity, and output column name.
 */
public final class SparkDateTimeDifferenceSettings {

    static final String CFG_FIRST_COLUMN = "firstColumn";
    static final String CFG_SECOND_MODE = "secondMode";
    static final String CFG_SECOND_COLUMN = "secondColumn";
    static final String CFG_FIXED_DATE_TIME = "fixedDateTime";
    static final String CFG_DIRECTION = "direction";
    static final String CFG_GRANULARITY = "granularity";
    static final String CFG_OUTPUT_COL_NAME = "outputColName";

    private String m_firstColumn = "";
    private String m_secondMode = "COLUMN";
    private String m_secondColumn = "";
    private String m_fixedDateTime = "";
    private String m_direction = "SECOND_MINUS_FIRST";
    private String m_granularity = "DAY";
    private String m_outputColName = "Difference";

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the first date/time column name */
    public String getFirstColumn() {
        return m_firstColumn;
    }

    /** @return the second value source mode (COLUMN, FIXED, CURRENT) */
    public String getSecondMode() {
        return m_secondMode;
    }

    /** @return the second date/time column name (for COLUMN mode) */
    public String getSecondColumn() {
        return m_secondColumn;
    }

    /** @return the fixed date/time string (for FIXED mode) */
    public String getFixedDateTime() {
        return m_fixedDateTime;
    }

    /** @return the difference direction (SECOND_MINUS_FIRST or FIRST_MINUS_SECOND) */
    public String getDirection() {
        return m_direction;
    }

    /** @return the result granularity (YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND) */
    public String getGranularity() {
        return m_granularity;
    }

    /** @return the output column name */
    public String getOutputColName() {
        return m_outputColName;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_FIRST_COLUMN, m_firstColumn);
        settings.addString(CFG_SECOND_MODE, m_secondMode);
        settings.addString(CFG_SECOND_COLUMN, m_secondColumn);
        settings.addString(CFG_FIXED_DATE_TIME, m_fixedDateTime);
        settings.addString(CFG_DIRECTION, m_direction);
        settings.addString(CFG_GRANULARITY, m_granularity);
        settings.addString(CFG_OUTPUT_COL_NAME, m_outputColName);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_FIRST_COLUMN);
        settings.getString(CFG_SECOND_MODE);
        settings.getString(CFG_OUTPUT_COL_NAME);
        // secondColumn and fixedDateTime may not be present in older configs
        if (settings.containsKey(CFG_SECOND_COLUMN)) {
            settings.getString(CFG_SECOND_COLUMN);
        }
        if (settings.containsKey(CFG_FIXED_DATE_TIME)) {
            settings.getString(CFG_FIXED_DATE_TIME);
        }
        if (settings.containsKey(CFG_DIRECTION)) {
            settings.getString(CFG_DIRECTION);
        }
        if (settings.containsKey(CFG_GRANULARITY)) {
            settings.getString(CFG_GRANULARITY);
        }
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_firstColumn = settings.getString(CFG_FIRST_COLUMN);
        m_secondMode = settings.getString(CFG_SECOND_MODE);
        m_outputColName = settings.getString(CFG_OUTPUT_COL_NAME);
        if (settings.containsKey(CFG_SECOND_COLUMN)) {
            m_secondColumn = settings.getString(CFG_SECOND_COLUMN);
        } else {
            m_secondColumn = "";
        }
        if (settings.containsKey(CFG_FIXED_DATE_TIME)) {
            m_fixedDateTime = settings.getString(CFG_FIXED_DATE_TIME);
        } else {
            m_fixedDateTime = "";
        }
        if (settings.containsKey(CFG_DIRECTION)) {
            m_direction = settings.getString(CFG_DIRECTION);
        } else {
            m_direction = "SECOND_MINUS_FIRST";
        }
        if (settings.containsKey(CFG_GRANULARITY)) {
            m_granularity = settings.getString(CFG_GRANULARITY);
        } else {
            m_granularity = "DAY";
        }
    }
}
