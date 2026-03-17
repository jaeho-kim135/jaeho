package org.knime.bigdata.spark.dx.node.preproc.stringreplacer;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark String Replacer node.
 * Stores all configuration values needed for find-and-replace operations.
 */
public final class SparkStringReplacerSettings {

    static final String CFG_COLUMN = "column";
    static final String CFG_PATTERN_TYPE = "patternType";
    static final String CFG_PATTERN = "pattern";
    static final String CFG_REPLACEMENT = "replacement";
    static final String CFG_CASE_SENSITIVE = "caseSensitive";
    static final String CFG_ENABLE_ESCAPING = "enableEscaping";
    static final String CFG_REPLACEMENT_STRATEGY = "replacementStrategy";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_NEW_COL_NAME = "newColName";

    private String m_column = "";
    private String m_patternType = "LITERAL";
    private String m_pattern = "";
    private String m_replacement = "";
    private boolean m_caseSensitive = true;
    private boolean m_enableEscaping = false;
    private String m_replacementStrategy = "WHOLE_STRING";
    private String m_appendOrReplace = "REPLACE";
    private String m_newColName = "Replaced";

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the target column name */
    public String getColumn() {
        return m_column;
    }

    /** @return the pattern type (LITERAL, WILDCARD, REGEX) */
    public String getPatternType() {
        return m_patternType;
    }

    /** @return the search pattern */
    public String getPattern() {
        return m_pattern;
    }

    /** @return the replacement text */
    public String getReplacement() {
        return m_replacement;
    }

    /** @return whether matching is case-sensitive */
    public boolean isCaseSensitive() {
        return m_caseSensitive;
    }

    /** @return whether wildcard escaping is enabled */
    public boolean isEnableEscaping() {
        return m_enableEscaping;
    }

    /** @return the replacement strategy (ALL_OCCURRENCES, WHOLE_STRING) */
    public String getReplacementStrategy() {
        return m_replacementStrategy;
    }

    /** @return the output mode (APPEND, REPLACE) */
    public String getAppendOrReplace() {
        return m_appendOrReplace;
    }

    /** @return true if the output mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(m_appendOrReplace);
    }

    /** @return true if the output mode is APPEND */
    public boolean isAppend() {
        return "APPEND".equals(m_appendOrReplace);
    }

    /** @return the new column name (for APPEND mode) */
    public String getNewColName() {
        return m_newColName;
    }

    /**
     * Returns the effective output column name.
     * In REPLACE mode, returns the target column name.
     * In APPEND mode, returns the new column name.
     *
     * @return the effective output column name
     */
    public String getEffectiveOutputColName() {
        return isReplace() ? m_column : m_newColName;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_COLUMN, m_column);
        settings.addString(CFG_PATTERN_TYPE, m_patternType);
        settings.addString(CFG_PATTERN, m_pattern);
        settings.addString(CFG_REPLACEMENT, m_replacement);
        settings.addBoolean(CFG_CASE_SENSITIVE, m_caseSensitive);
        settings.addBoolean(CFG_ENABLE_ESCAPING, m_enableEscaping);
        settings.addString(CFG_REPLACEMENT_STRATEGY, m_replacementStrategy);
        settings.addString(CFG_APPEND_OR_REPLACE, m_appendOrReplace);
        settings.addString(CFG_NEW_COL_NAME, m_newColName);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_COLUMN);
        settings.getString(CFG_PATTERN_TYPE);
        settings.getString(CFG_PATTERN);
        settings.getString(CFG_REPLACEMENT);
        settings.getBoolean(CFG_CASE_SENSITIVE);
        if (settings.containsKey(CFG_ENABLE_ESCAPING)) {
            settings.getBoolean(CFG_ENABLE_ESCAPING);
        }
        settings.getString(CFG_REPLACEMENT_STRATEGY);
        settings.getString(CFG_APPEND_OR_REPLACE);
        if (settings.containsKey(CFG_NEW_COL_NAME)) {
            settings.getString(CFG_NEW_COL_NAME);
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
        m_patternType = settings.getString(CFG_PATTERN_TYPE);
        m_pattern = settings.getString(CFG_PATTERN);
        m_replacement = settings.getString(CFG_REPLACEMENT);
        m_caseSensitive = settings.getBoolean(CFG_CASE_SENSITIVE);
        if (settings.containsKey(CFG_ENABLE_ESCAPING)) {
            m_enableEscaping = settings.getBoolean(CFG_ENABLE_ESCAPING);
        } else {
            m_enableEscaping = false;
        }
        m_replacementStrategy = settings.getString(CFG_REPLACEMENT_STRATEGY);
        m_appendOrReplace = settings.getString(CFG_APPEND_OR_REPLACE);
        if (settings.containsKey(CFG_NEW_COL_NAME)) {
            m_newColName = settings.getString(CFG_NEW_COL_NAME);
        } else {
            m_newColName = "Replaced";
        }
    }
}
