package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsRO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsWO;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.scripting.editor.GenericSettingsIOManager;
import org.knime.scripting.editor.ScriptingNodeSettings;

/**
 * WebUI settings adapter for the Spark Rule Engine node.
 * Bridges between NodeSettings format ({@link SparkRuleEngineSettings})
 * and the JSON format consumed by the WebUI frontend.
 */
@SuppressWarnings("restriction")
final class SparkRuleEngineWebSettings extends ScriptingNodeSettings implements GenericSettingsIOManager {

    private static final String JSON_KEY_RULES = "rules";
    private static final String JSON_KEY_DEFAULT_VALUE = "defaultValue";
    private static final String JSON_KEY_DEFAULT_IS_MISSING = "defaultIsMissing";
    private static final String JSON_KEY_APPEND_OR_REPLACE = "appendOrReplace";
    private static final String JSON_KEY_OUTPUT_COLUMN_NAME = "outputColumnName";
    private static final String JSON_KEY_REPLACE_COLUMN = "replaceColumn";

    private String m_rules = "";
    private String m_defaultValue = "";
    private boolean m_defaultIsMissing = true;
    private String m_appendOrReplace = "APPEND";
    private String m_outputColumnName = "Rule Result";
    private String m_replaceColumn = "";

    SparkRuleEngineWebSettings() {
        super(SettingsType.MODEL);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(SparkRuleEngineSettings.CFG_RULES)) {
            return; // keep defaults
        }
        m_rules = settings.getString(SparkRuleEngineSettings.CFG_RULES);
        m_defaultValue = settings.getString(SparkRuleEngineSettings.CFG_DEFAULT_VALUE);
        m_defaultIsMissing = settings.getBoolean(SparkRuleEngineSettings.CFG_DEFAULT_IS_MISSING);
        m_appendOrReplace = settings.getString(SparkRuleEngineSettings.CFG_APPEND_OR_REPLACE);
        m_outputColumnName = settings.getString(SparkRuleEngineSettings.CFG_OUTPUT_COLUMN_NAME);
        m_replaceColumn = settings.getString(SparkRuleEngineSettings.CFG_REPLACE_COLUMN);
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(SparkRuleEngineSettings.CFG_RULES, m_rules);
        settings.addString(SparkRuleEngineSettings.CFG_DEFAULT_VALUE, m_defaultValue);
        settings.addBoolean(SparkRuleEngineSettings.CFG_DEFAULT_IS_MISSING, m_defaultIsMissing);
        settings.addString(SparkRuleEngineSettings.CFG_APPEND_OR_REPLACE, m_appendOrReplace);
        settings.addString(SparkRuleEngineSettings.CFG_OUTPUT_COLUMN_NAME, m_outputColumnName);
        settings.addString(SparkRuleEngineSettings.CFG_REPLACE_COLUMN, m_replaceColumn);
        settings.addBoolean(SparkRuleEngineSettings.CFG_CONFIGURED, true);
    }

    @Override
    public Map<String, Object> convertNodeSettingsToMap(
            final Map<SettingsType, NodeAndVariableSettingsRO> settings) throws InvalidSettingsException {
        loadSettingsFrom(settings);

        final Map<String, Object> map = new HashMap<>();
        map.put(JSON_KEY_RULES, m_rules);
        map.put(JSON_KEY_DEFAULT_VALUE, m_defaultValue);
        map.put(JSON_KEY_DEFAULT_IS_MISSING, m_defaultIsMissing);
        map.put(JSON_KEY_APPEND_OR_REPLACE, m_appendOrReplace);
        map.put(JSON_KEY_OUTPUT_COLUMN_NAME, m_outputColumnName);
        map.put(JSON_KEY_REPLACE_COLUMN, m_replaceColumn);
        return map;
    }

    @Override
    public void writeMapToNodeSettings(final Map<String, Object> data,
            final Map<SettingsType, NodeAndVariableSettingsRO> previousSettings,
            final Map<SettingsType, NodeAndVariableSettingsWO> settings) throws InvalidSettingsException {

        m_rules = getString(data, JSON_KEY_RULES, "");
        m_defaultValue = getString(data, JSON_KEY_DEFAULT_VALUE, "");
        m_defaultIsMissing = getBoolean(data, JSON_KEY_DEFAULT_IS_MISSING, true);
        m_appendOrReplace = getString(data, JSON_KEY_APPEND_OR_REPLACE, "APPEND");
        m_outputColumnName = getString(data, JSON_KEY_OUTPUT_COLUMN_NAME, "Rule Result");
        m_replaceColumn = getString(data, JSON_KEY_REPLACE_COLUMN, "");

        // Validate appendOrReplace
        if (!"APPEND".equals(m_appendOrReplace) && !"REPLACE".equals(m_appendOrReplace)) {
            throw new InvalidSettingsException(
                "Invalid output mode: '" + m_appendOrReplace + "'. Must be APPEND or REPLACE.");
        }

        saveSettingsTo(settings.get(SettingsType.MODEL));
        copyVariableSettings(previousSettings, settings);
    }

    private static String getString(final Map<String, Object> data, final String key, final String def) {
        final Object val = data.get(key);
        return val != null ? val.toString() : def;
    }

    private static boolean getBoolean(final Map<String, Object> data, final String key, final boolean def) {
        final Object val = data.get(key);
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        return def;
    }
}
