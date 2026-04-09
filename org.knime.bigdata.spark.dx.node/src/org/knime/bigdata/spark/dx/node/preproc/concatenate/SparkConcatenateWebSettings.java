package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsRO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsWO;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.scripting.editor.GenericSettingsIOManager;
import org.knime.scripting.editor.ScriptingNodeSettings;

/**
 * WebUI settings adapter for the Spark Concatenate node.
 * Bridges between the existing NodeSettings format ({@link SparkConcatenateSettings})
 * and the JSON format consumed by the WebUI frontend.
 */
@SuppressWarnings("restriction")
final class SparkConcatenateWebSettings extends ScriptingNodeSettings implements GenericSettingsIOManager {

    private static final String JSON_KEY_LEFT_COLUMNS = "leftColumns";
    private static final String JSON_KEY_RIGHT_COLUMNS = "rightColumns";
    private static final String JSON_KEY_UNMATCHED_LEFT = "unmatchedLeftAction";
    private static final String JSON_KEY_UNMATCHED_RIGHT = "unmatchedRightAction";

    private List<String> m_leftColumns;
    private List<String> m_rightColumns;
    private String m_unmatchedLeftAction;
    private String m_unmatchedRightAction;
    private boolean m_nodeConfigured;

    SparkConcatenateWebSettings() {
        super(SettingsType.MODEL);
        m_leftColumns = new ArrayList<>();
        m_rightColumns = new ArrayList<>();
        m_unmatchedLeftAction = "FILL_NULL";
        m_unmatchedRightAction = "FILL_NULL";
        m_nodeConfigured = false;
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(SparkConcatenateSettings.CFG_LEFT_COLUMNS)) {
            return;
        }
        final String[] lefts = settings.getStringArray(SparkConcatenateSettings.CFG_LEFT_COLUMNS);
        final String[] rights = settings.containsKey(SparkConcatenateSettings.CFG_RIGHT_COLUMNS)
            ? settings.getStringArray(SparkConcatenateSettings.CFG_RIGHT_COLUMNS) : new String[0];
        m_leftColumns = new ArrayList<>();
        m_rightColumns = new ArrayList<>();
        for (int i = 0; i < lefts.length; i++) {
            m_leftColumns.add(lefts[i]);
            m_rightColumns.add(i < rights.length ? rights[i] : "");
        }
        m_unmatchedLeftAction = settings.containsKey(SparkConcatenateSettings.CFG_UNMATCHED_LEFT)
            ? settings.getString(SparkConcatenateSettings.CFG_UNMATCHED_LEFT) : "FILL_NULL";
        m_unmatchedRightAction = settings.containsKey(SparkConcatenateSettings.CFG_UNMATCHED_RIGHT)
            ? settings.getString(SparkConcatenateSettings.CFG_UNMATCHED_RIGHT) : "FILL_NULL";
        m_nodeConfigured = settings.containsKey(SparkConcatenateSettings.CFG_CONFIGURED);
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addStringArray(SparkConcatenateSettings.CFG_LEFT_COLUMNS,
            m_leftColumns.toArray(new String[0]));
        settings.addStringArray(SparkConcatenateSettings.CFG_RIGHT_COLUMNS,
            m_rightColumns.toArray(new String[0]));
        settings.addString(SparkConcatenateSettings.CFG_UNMATCHED_LEFT, m_unmatchedLeftAction);
        settings.addString(SparkConcatenateSettings.CFG_UNMATCHED_RIGHT, m_unmatchedRightAction);
        settings.addBoolean(SparkConcatenateSettings.CFG_CONFIGURED, true);
    }

    @Override
    public Map<String, Object> convertNodeSettingsToMap(
            final Map<SettingsType, NodeAndVariableSettingsRO> settings) throws InvalidSettingsException {
        loadSettingsFrom(settings);

        final Map<String, Object> map = new HashMap<>();
        map.put(JSON_KEY_LEFT_COLUMNS, m_leftColumns);
        map.put(JSON_KEY_RIGHT_COLUMNS, m_rightColumns);
        map.put(JSON_KEY_UNMATCHED_LEFT, m_unmatchedLeftAction);
        map.put(JSON_KEY_UNMATCHED_RIGHT, m_unmatchedRightAction);
        map.put("nodeConfigured", m_nodeConfigured);
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeMapToNodeSettings(final Map<String, Object> data,
            final Map<SettingsType, NodeAndVariableSettingsRO> previousSettings,
            final Map<SettingsType, NodeAndVariableSettingsWO> settings) throws InvalidSettingsException {

        final Object rawLeft = data.get(JSON_KEY_LEFT_COLUMNS);
        final Object rawRight = data.get(JSON_KEY_RIGHT_COLUMNS);
        final Object rawUnmatchedLeft = data.get(JSON_KEY_UNMATCHED_LEFT);
        final Object rawUnmatchedRight = data.get(JSON_KEY_UNMATCHED_RIGHT);

        if (rawLeft == null || rawRight == null) {
            throw new InvalidSettingsException("Missing column mapping data from the dialog.");
        }

        m_leftColumns = ((List<Object>) rawLeft).stream()
            .map(Object::toString).collect(Collectors.toList());
        m_rightColumns = ((List<Object>) rawRight).stream()
            .map(Object::toString).collect(Collectors.toList());

        if (m_leftColumns.size() != m_rightColumns.size()) {
            throw new InvalidSettingsException(
                "Left and right column arrays must have the same length.");
        }

        m_unmatchedLeftAction = rawUnmatchedLeft != null ? rawUnmatchedLeft.toString() : "FILL_NULL";
        m_unmatchedRightAction = rawUnmatchedRight != null ? rawUnmatchedRight.toString() : "FILL_NULL";

        saveSettingsTo(settings.get(SettingsType.MODEL));
        copyVariableSettings(previousSettings, settings);
    }
}
