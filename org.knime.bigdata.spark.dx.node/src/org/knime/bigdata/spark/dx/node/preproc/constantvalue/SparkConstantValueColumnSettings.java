package org.knime.bigdata.spark.dx.node.preproc.constantvalue;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Constant Value Column node.
 * Stores column name, value type, value, isMissing flag, and append/replace mode.
 */
public final class SparkConstantValueColumnSettings {

    static final String CFG_COLUMN_NAME = "columnName";
    static final String CFG_VALUE_TYPE = "valueType";
    static final String CFG_VALUE = "value";
    static final String CFG_IS_MISSING = "isMissing";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_REPLACE_COLUMN = "replaceColumn";

    private String m_columnName = "constant";
    private String m_valueType = "STRING";
    private String m_value = "";
    private boolean m_isMissing = false;
    private String m_appendOrReplace = "APPEND";
    private String m_replaceColumn = "";

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the output column name (for APPEND mode) */
    public String getColumnName() {
        return m_columnName;
    }

    /** @return the value type as string (STRING, INTEGER, LONG, DOUBLE, BOOLEAN, DATE, TIMESTAMP) */
    public String getValueType() {
        return m_valueType;
    }

    /** @return the constant value as a string */
    public String getValue() {
        return m_value;
    }

    /** @return true if the column should be filled with missing (null) values */
    public boolean isMissing() {
        return m_isMissing;
    }

    /** @return the output mode (APPEND or REPLACE) */
    public String getAppendOrReplace() {
        return m_appendOrReplace;
    }

    /** @return true if the output mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(m_appendOrReplace);
    }

    /** @return the column to replace (for REPLACE mode) */
    public String getReplaceColumn() {
        return m_replaceColumn;
    }

    /**
     * Returns the effective output column name.
     * In APPEND mode, returns {@link #getColumnName()}.
     * In REPLACE mode, returns {@link #getReplaceColumn()}.
     *
     * @return the effective output column name
     */
    public String getEffectiveColumnName() {
        return isReplace() ? m_replaceColumn : m_columnName;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_COLUMN_NAME, m_columnName);
        settings.addString(CFG_VALUE_TYPE, m_valueType);
        settings.addString(CFG_VALUE, m_value);
        settings.addBoolean(CFG_IS_MISSING, m_isMissing);
        settings.addString(CFG_APPEND_OR_REPLACE, m_appendOrReplace);
        settings.addString(CFG_REPLACE_COLUMN, m_replaceColumn);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_COLUMN_NAME);
        settings.getString(CFG_VALUE_TYPE);
        settings.getString(CFG_VALUE);
        settings.getBoolean(CFG_IS_MISSING);
        settings.getString(CFG_APPEND_OR_REPLACE);
        if (settings.containsKey(CFG_REPLACE_COLUMN)) {
            settings.getString(CFG_REPLACE_COLUMN);
        }
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_columnName = settings.getString(CFG_COLUMN_NAME);
        m_valueType = settings.getString(CFG_VALUE_TYPE);
        m_value = settings.getString(CFG_VALUE);
        m_isMissing = settings.getBoolean(CFG_IS_MISSING);
        m_appendOrReplace = settings.getString(CFG_APPEND_OR_REPLACE);
        if (settings.containsKey(CFG_REPLACE_COLUMN)) {
            m_replaceColumn = settings.getString(CFG_REPLACE_COLUMN);
        } else {
            m_replaceColumn = "";
        }
    }
}
