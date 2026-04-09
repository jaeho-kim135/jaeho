package org.knime.bigdata.spark.dx.node.preproc.editcolumn;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Edit Column node.
 * Stores flat arrays of source column names, new names, and new types.
 * The array order determines the output column order for edited columns.
 */
public final class SparkEditColumnSettings {

    static final String CFG_SOURCE_COLUMNS = "sourceColumns";
    static final String CFG_NEW_NAMES = "newNames";
    static final String CFG_NEW_TYPES = "newTypes";

    private String[] m_sourceColumns = new String[0];
    private String[] m_newNames = new String[0];
    private String[] m_newTypes = new String[0];

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the source column names (array order = output order) */
    public String[] getSourceColumns() {
        return m_sourceColumns;
    }

    /** @return the new column names (empty = keep original) */
    public String[] getNewNames() {
        return m_newNames;
    }

    /** @return the new data types (KEEP = keep original) */
    public String[] getNewTypes() {
        return m_newTypes;
    }

    // ── Setters ──────────────────────────────────────────────────────────────

    /** Sets the source column names. */
    public void setSourceColumns(final String[] sourceColumns) {
        m_sourceColumns = sourceColumns;
    }

    /** Sets the new column names. */
    public void setNewNames(final String[] newNames) {
        m_newNames = newNames;
    }

    /** Sets the new data types. */
    public void setNewTypes(final String[] newTypes) {
        m_newTypes = newTypes;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addStringArray(CFG_SOURCE_COLUMNS, m_sourceColumns);
        settings.addStringArray(CFG_NEW_NAMES, m_newNames);
        settings.addStringArray(CFG_NEW_TYPES, m_newTypes);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_SOURCE_COLUMNS)) {
            return; // Not yet configured
        }
        final String[] src = settings.getStringArray(CFG_SOURCE_COLUMNS);
        // Guard for old-format workflows where newNames/newTypes may not exist
        if (!settings.containsKey(CFG_NEW_NAMES) || !settings.containsKey(CFG_NEW_TYPES)) {
            return;
        }
        final String[] names = settings.getStringArray(CFG_NEW_NAMES);
        final String[] types = settings.getStringArray(CFG_NEW_TYPES);
        if (src.length != names.length || src.length != types.length) {
            throw new InvalidSettingsException(
                "Inconsistent settings: sourceColumns, newNames, and newTypes arrays must have the same length.");
        }
        for (final String s : src) {
            if (s == null) {
                throw new InvalidSettingsException("sourceColumns contains a null entry.");
            }
        }
        for (final String t : types) {
            if (t != null && !t.isEmpty() && !"KEEP".equals(t) && !"STRING".equals(t)
                    && !"INTEGER".equals(t) && !"LONG".equals(t) && !"DOUBLE".equals(t)
                    && !"FLOAT".equals(t) && !"BOOLEAN".equals(t) && !"DATE".equals(t)
                    && !"TIMESTAMP".equals(t)) {
                throw new InvalidSettingsException("Unknown type: '" + t + "'");
            }
        }
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_SOURCE_COLUMNS)) {
            return; // Keep defaults
        }
        m_sourceColumns = settings.getStringArray(CFG_SOURCE_COLUMNS);
        m_newNames = settings.containsKey(CFG_NEW_NAMES)
            ? settings.getStringArray(CFG_NEW_NAMES) : new String[0];
        m_newTypes = settings.containsKey(CFG_NEW_TYPES)
            ? settings.getStringArray(CFG_NEW_TYPES) : new String[0];
    }
}
