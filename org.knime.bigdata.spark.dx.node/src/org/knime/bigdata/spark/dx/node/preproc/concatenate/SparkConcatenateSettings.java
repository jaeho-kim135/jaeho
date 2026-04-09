package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Concatenate node.
 * Stores column mapping arrays and unmatched column handling actions.
 */
public final class SparkConcatenateSettings {

    static final String CFG_LEFT_COLUMNS = "leftColumns";
    static final String CFG_RIGHT_COLUMNS = "rightColumns";
    static final String CFG_UNMATCHED_LEFT = "unmatchedLeftAction";
    static final String CFG_UNMATCHED_RIGHT = "unmatchedRightAction";
    static final String CFG_CONFIGURED = "nodeConfigured";

    private String[] m_leftColumns = new String[0];
    private String[] m_rightColumns = new String[0];
    private String m_unmatchedLeftAction = "FILL_NULL";
    private String m_unmatchedRightAction = "FILL_NULL";
    private boolean m_nodeConfigured = false;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the left column names in the mapping */
    public String[] getLeftColumns() {
        return m_leftColumns;
    }

    /** @return the right column names in the mapping (empty = same name) */
    public String[] getRightColumns() {
        return m_rightColumns;
    }

    /** @return the unmatched left column action (FILL_NULL or EXCLUDE) */
    public String getUnmatchedLeftAction() {
        return m_unmatchedLeftAction;
    }

    /** @return the unmatched right column action (FILL_NULL or EXCLUDE) */
    public String getUnmatchedRightAction() {
        return m_unmatchedRightAction;
    }

    /** @return true if the node has been configured (dialog OK'd at least once) */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    // ── Setters ──────────────────────────────────────────────────────────────

    public void setLeftColumns(final String[] leftColumns) {
        m_leftColumns = leftColumns;
    }

    public void setRightColumns(final String[] rightColumns) {
        m_rightColumns = rightColumns;
    }

    public void setUnmatchedLeftAction(final String action) {
        m_unmatchedLeftAction = action;
    }

    public void setUnmatchedRightAction(final String action) {
        m_unmatchedRightAction = action;
    }

    public void setNodeConfigured(final boolean configured) {
        m_nodeConfigured = configured;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addStringArray(CFG_LEFT_COLUMNS, m_leftColumns);
        settings.addStringArray(CFG_RIGHT_COLUMNS, m_rightColumns);
        settings.addString(CFG_UNMATCHED_LEFT, m_unmatchedLeftAction);
        settings.addString(CFG_UNMATCHED_RIGHT, m_unmatchedRightAction);
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final String[] left = settings.getStringArray(CFG_LEFT_COLUMNS);
        final String[] right = settings.getStringArray(CFG_RIGHT_COLUMNS);
        if (left.length != right.length) {
            throw new InvalidSettingsException(
                "Inconsistent settings: leftColumns and rightColumns arrays must have the same length.");
        }
        final String unmatchedLeft = settings.getString(CFG_UNMATCHED_LEFT);
        if (!"FILL_NULL".equals(unmatchedLeft) && !"EXCLUDE".equals(unmatchedLeft)) {
            throw new InvalidSettingsException(
                "Invalid unmatched left column action: '" + unmatchedLeft + "'. Must be FILL_NULL or EXCLUDE.");
        }
        final String unmatchedRight = settings.getString(CFG_UNMATCHED_RIGHT);
        if (!"FILL_NULL".equals(unmatchedRight) && !"EXCLUDE".equals(unmatchedRight)) {
            throw new InvalidSettingsException(
                "Invalid unmatched right column action: '" + unmatchedRight + "'. Must be FILL_NULL or EXCLUDE.");
        }
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_leftColumns = settings.containsKey(CFG_LEFT_COLUMNS)
            ? settings.getStringArray(CFG_LEFT_COLUMNS) : new String[0];
        m_rightColumns = settings.containsKey(CFG_RIGHT_COLUMNS)
            ? settings.getStringArray(CFG_RIGHT_COLUMNS) : new String[0];
        m_unmatchedLeftAction = settings.containsKey(CFG_UNMATCHED_LEFT)
            ? settings.getString(CFG_UNMATCHED_LEFT) : "FILL_NULL";
        m_unmatchedRightAction = settings.containsKey(CFG_UNMATCHED_RIGHT)
            ? settings.getString(CFG_UNMATCHED_RIGHT) : "FILL_NULL";
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }
}
