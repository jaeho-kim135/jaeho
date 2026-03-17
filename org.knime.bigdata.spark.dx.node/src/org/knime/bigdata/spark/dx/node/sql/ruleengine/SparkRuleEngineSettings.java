package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Rule Engine node.
 * Stores rule text, default value, and output column configuration.
 */
public final class SparkRuleEngineSettings {

    static final String CFG_RULES = "rules";
    static final String CFG_DEFAULT_VALUE = "defaultValue";
    static final String CFG_DEFAULT_IS_MISSING = "defaultIsMissing";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_OUTPUT_COLUMN_NAME = "outputColumnName";
    static final String CFG_REPLACE_COLUMN = "replaceColumn";
    static final String CFG_CONFIGURED = "nodeConfigured";

    private final SettingsModelString m_rules =
        new SettingsModelString(CFG_RULES, "");

    private final SettingsModelString m_defaultValue =
        new SettingsModelString(CFG_DEFAULT_VALUE, "");

    private final SettingsModelBoolean m_defaultIsMissing =
        new SettingsModelBoolean(CFG_DEFAULT_IS_MISSING, true);

    private final SettingsModelString m_appendOrReplace =
        new SettingsModelString(CFG_APPEND_OR_REPLACE, "APPEND");

    private final SettingsModelString m_outputColumnName =
        new SettingsModelString(CFG_OUTPUT_COLUMN_NAME, "Rule Result");

    private final SettingsModelString m_replaceColumn =
        new SettingsModelString(CFG_REPLACE_COLUMN, "");

    private boolean m_nodeConfigured = false;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the rules text (multi-line, one rule per line) */
    public String getRules() {
        return m_rules.getStringValue();
    }

    /** @return the default value for non-matching rows */
    public String getDefaultValue() {
        return m_defaultValue.getStringValue();
    }

    /** @return true if the default value should be MISSING (null) */
    public boolean isDefaultMissing() {
        return m_defaultIsMissing.getBooleanValue();
    }

    /** @return "APPEND" or "REPLACE" */
    public String getAppendOrReplace() {
        return m_appendOrReplace.getStringValue();
    }

    /** @return true if mode is REPLACE */
    public boolean isReplace() {
        return "REPLACE".equals(m_appendOrReplace.getStringValue());
    }

    /** @return the output column name (for APPEND mode) */
    public String getOutputColumnName() {
        return m_outputColumnName.getStringValue();
    }

    /** @return the column to replace (for REPLACE mode) */
    public String getReplaceColumn() {
        return m_replaceColumn.getStringValue();
    }

    /** @return the effective output column name based on mode */
    public String getEffectiveOutputColumn() {
        if (isReplace()) {
            return m_replaceColumn.getStringValue();
        }
        return m_outputColumnName.getStringValue();
    }

    /** @return true if the node has been configured (dialog was opened and OK'd) */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    /** @param configured set to true when user accepts the dialog */
    public void setNodeConfigured(final boolean configured) {
        m_nodeConfigured = configured;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_rules.saveSettingsTo(settings);
        m_defaultValue.saveSettingsTo(settings);
        m_defaultIsMissing.saveSettingsTo(settings);
        m_appendOrReplace.saveSettingsTo(settings);
        m_outputColumnName.saveSettingsTo(settings);
        m_replaceColumn.saveSettingsTo(settings);
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
        m_rules.validateSettings(settings);
        m_defaultValue.validateSettings(settings);
        m_defaultIsMissing.validateSettings(settings);
        m_appendOrReplace.validateSettings(settings);
        m_outputColumnName.validateSettings(settings);
        m_replaceColumn.validateSettings(settings);
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_rules.loadSettingsFrom(settings);
        m_defaultValue.loadSettingsFrom(settings);
        m_defaultIsMissing.loadSettingsFrom(settings);
        m_appendOrReplace.loadSettingsFrom(settings);
        m_outputColumnName.loadSettingsFrom(settings);
        m_replaceColumn.loadSettingsFrom(settings);
        // WebUI persistors write field-specific keys (e.g. CFG_RULES) but not CFG_CONFIGURED,
        // so we also check for a key that WebUI always writes to detect dialog acceptance.
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED) || settings.containsKey(CFG_RULES);
    }
}
