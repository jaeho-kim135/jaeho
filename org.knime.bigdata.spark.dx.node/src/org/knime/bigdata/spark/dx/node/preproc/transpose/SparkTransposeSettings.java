package org.knime.bigdata.spark.dx.node.preproc.transpose;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Table Transposer node.
 * Manages the maximum rows limit and optional ID column configuration.
 */
public final class SparkTransposeSettings {

    static final String CFG_MAX_ROWS = "maxRows";
    static final String CFG_ID_COLUMN = "idColumn";

    private int m_maxRows = 1000;
    private String m_idColumn = "";

    /** @return the maximum number of rows allowed for transpose */
    public int getMaxRows() {
        return m_maxRows;
    }

    /** @param maxRows the maximum number of rows to set */
    public void setMaxRows(final int maxRows) {
        m_maxRows = maxRows;
    }

    /** @return the ID column name, or empty string if not specified */
    public String getIdColumn() {
        return m_idColumn;
    }

    /** @param idColumn the ID column name to set */
    public void setIdColumn(final String idColumn) {
        m_idColumn = idColumn;
    }

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addInt(CFG_MAX_ROWS, m_maxRows);
        settings.addString(CFG_ID_COLUMN, m_idColumn);
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final int maxRows = settings.getInt(CFG_MAX_ROWS);
        if (maxRows < 1) {
            throw new InvalidSettingsException("Maximum rows must be at least 1.");
        }
        if (maxRows > 100000) {
            throw new InvalidSettingsException("Maximum rows must not exceed 100,000.");
        }
        // idColumn is optional – tolerate missing key for backward compatibility
        if (settings.containsKey(CFG_ID_COLUMN)) {
            settings.getString(CFG_ID_COLUMN);
        }
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_maxRows = settings.getInt(CFG_MAX_ROWS);
        m_idColumn = settings.containsKey(CFG_ID_COLUMN)
            ? settings.getString(CFG_ID_COLUMN) : "";
    }
}
