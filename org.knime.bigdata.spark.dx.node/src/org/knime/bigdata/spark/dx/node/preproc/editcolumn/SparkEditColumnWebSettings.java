package org.knime.bigdata.spark.dx.node.preproc.editcolumn;

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
 * WebUI settings adapter for the Spark Edit Column node.
 * Bridges between the existing NodeSettings format ({@link SparkEditColumnSettings})
 * and the JSON format consumed by the WebUI frontend.
 *
 * <p>NodeSettings format: parallel arrays {@code sourceColumns[]}, {@code newNames[]}, {@code newTypes[]}.
 * <p>JSON format: same keys with List values. {@code newTypes} uses empty string for "keep original"
 * (converted to/from "KEEP" in NodeSettings).
 */
@SuppressWarnings("restriction")
final class SparkEditColumnWebSettings extends ScriptingNodeSettings implements GenericSettingsIOManager {

    private List<String> m_sourceColumns;
    private List<String> m_newNames;
    private List<String> m_newTypes;

    SparkEditColumnWebSettings() {
        super(SettingsType.MODEL);
        m_sourceColumns = new ArrayList<>();
        m_newNames = new ArrayList<>();
        m_newTypes = new ArrayList<>();
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(SparkEditColumnSettings.CFG_SOURCE_COLUMNS)) {
            return;
        }
        final String[] src = settings.getStringArray(SparkEditColumnSettings.CFG_SOURCE_COLUMNS);
        final String[] names = settings.containsKey(SparkEditColumnSettings.CFG_NEW_NAMES)
            ? settings.getStringArray(SparkEditColumnSettings.CFG_NEW_NAMES) : new String[0];
        final String[] types = settings.containsKey(SparkEditColumnSettings.CFG_NEW_TYPES)
            ? settings.getStringArray(SparkEditColumnSettings.CFG_NEW_TYPES) : new String[0];

        m_sourceColumns = new ArrayList<>();
        m_newNames = new ArrayList<>();
        m_newTypes = new ArrayList<>();
        for (int i = 0; i < src.length; i++) {
            m_sourceColumns.add(src[i]);
            m_newNames.add(i < names.length ? names[i] : "");
            final String t = (i < types.length) ? types[i] : "KEEP";
            m_newTypes.add("KEEP".equals(t) ? "" : t);
        }
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addStringArray(SparkEditColumnSettings.CFG_SOURCE_COLUMNS,
            m_sourceColumns.toArray(new String[0]));
        settings.addStringArray(SparkEditColumnSettings.CFG_NEW_NAMES,
            m_newNames.toArray(new String[0]));
        // Convert empty types back to KEEP for NodeSettings
        final String[] types = new String[m_newTypes.size()];
        for (int i = 0; i < m_newTypes.size(); i++) {
            final String t = m_newTypes.get(i);
            types[i] = (t == null || t.isEmpty()) ? "KEEP" : t;
        }
        settings.addStringArray(SparkEditColumnSettings.CFG_NEW_TYPES, types);
    }

    @Override
    public Map<String, Object> convertNodeSettingsToMap(
            final Map<SettingsType, NodeAndVariableSettingsRO> settings) throws InvalidSettingsException {
        loadSettingsFrom(settings);

        final Map<String, Object> map = new HashMap<>();
        map.put("sourceColumns", m_sourceColumns);
        map.put("newNames", m_newNames);
        map.put("newTypes", m_newTypes);
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeMapToNodeSettings(final Map<String, Object> data,
            final Map<SettingsType, NodeAndVariableSettingsRO> previousSettings,
            final Map<SettingsType, NodeAndVariableSettingsWO> settings) throws InvalidSettingsException {

        final Object rawSrc = data.get("sourceColumns");
        final Object rawNames = data.get("newNames");
        final Object rawTypes = data.get("newTypes");

        if (rawSrc == null) {
            throw new InvalidSettingsException("Missing column data from the dialog.");
        }

        m_sourceColumns = ((List<Object>) rawSrc).stream()
            .map(Object::toString).collect(Collectors.toList());
        m_newNames = rawNames != null
            ? ((List<Object>) rawNames).stream().map(Object::toString).collect(Collectors.toList())
            : new ArrayList<>();
        m_newTypes = rawTypes != null
            ? ((List<Object>) rawTypes).stream().map(Object::toString).collect(Collectors.toList())
            : new ArrayList<>();

        // Pad arrays to same length if needed
        while (m_newNames.size() < m_sourceColumns.size()) {
            m_newNames.add("");
        }
        while (m_newTypes.size() < m_sourceColumns.size()) {
            m_newTypes.add("");
        }

        saveSettingsTo(settings.get(SettingsType.MODEL));
        copyVariableSettings(previousSettings, settings);
    }
}
