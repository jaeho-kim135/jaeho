package org.knime.bigdata.spark.dx.node.preproc.stringmanip;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark String Manipulation node.
 * Stores a single Spark SQL expression and output mode settings.
 */
public final class SparkStringManipSettings {

    static final String CFG_EXPRESSION = "expression";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_OUTPUT_COL_NAME = "outputColName";
    static final String CFG_REPLACE_COLUMN = "replaceColumn";

    private String m_expression = "";
    private String m_appendOrReplace = "APPEND";
    private String m_outputColName = "StringManip";
    private String m_replaceColumn = "";

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the Spark SQL string expression */
    public String getExpression() {
        return m_expression;
    }

    /** @return the output mode (APPEND or REPLACE) */
    public String getAppendOrReplace() {
        return m_appendOrReplace;
    }

    /** @return true if the output mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(m_appendOrReplace);
    }

    /** @return the output column name (for APPEND mode) */
    public String getOutputColName() {
        return m_outputColName;
    }

    /** @return the column to replace (for REPLACE mode) */
    public String getReplaceColumn() {
        return m_replaceColumn;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_EXPRESSION, m_expression);
        settings.addString(CFG_APPEND_OR_REPLACE, m_appendOrReplace);
        settings.addString(CFG_OUTPUT_COL_NAME, m_outputColName);
        settings.addString(CFG_REPLACE_COLUMN, m_replaceColumn);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_EXPRESSION);
        settings.getString(CFG_APPEND_OR_REPLACE);
        settings.getString(CFG_OUTPUT_COL_NAME);
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
        m_expression = settings.getString(CFG_EXPRESSION);
        m_appendOrReplace = settings.getString(CFG_APPEND_OR_REPLACE);
        m_outputColName = settings.getString(CFG_OUTPUT_COL_NAME);
        if (settings.containsKey(CFG_REPLACE_COLUMN)) {
            m_replaceColumn = settings.getString(CFG_REPLACE_COLUMN);
        } else {
            m_replaceColumn = "";
        }
    }
}
