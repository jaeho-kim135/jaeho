package org.knime.bigdata.spark.dx.node.preproc.lagcolumn;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

public final class SparkLagColumnSettings {

    static final String CFG_COLUMN = "column";
    static final String CFG_ORDER_COLUMN = "orderColumn";
    static final String CFG_DIRECTION = "direction";
    static final String CFG_NUM_COPIES = "numCopies";
    static final String CFG_LAG_INTERVAL = "lagInterval";
    static final String CFG_GROUP_COLUMNS = "groupColumns";
    static final String CFG_SKIP_INCOMPLETE = "skipIncompleteRows";
    static final String CFG_CONFIGURED = "nodeConfigured";

    public static final String DIR_LAG = "LAG";
    public static final String DIR_LEAD = "LEAD";

    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";

    private String m_column = "";
    private String m_orderColumn = "";
    private String m_direction = DIR_LAG;
    private int m_numCopies = 1;
    private int m_lagInterval = 1;
    private List<String> m_groupColumns = List.of();
    private boolean m_skipIncompleteRows = false;
    private boolean m_nodeConfigured = false;

    public String getColumn() { return m_column; }
    public String getOrderColumn() { return m_orderColumn; }
    public String getDirection() { return m_direction; }
    public int getNumCopies() { return m_numCopies; }
    public int getLagInterval() { return m_lagInterval; }
    public List<String> getGroupColumns() { return m_groupColumns; }
    public boolean isSkipIncompleteRows() { return m_skipIncompleteRows; }
    public boolean isNodeConfigured() { return m_nodeConfigured; }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_COLUMN, m_column);
        settings.addString(CFG_ORDER_COLUMN, m_orderColumn);
        settings.addString(CFG_DIRECTION, m_direction);
        settings.addInt(CFG_NUM_COPIES, m_numCopies);
        settings.addInt(CFG_LAG_INTERVAL, m_lagInterval);
        writeColumnFilter(settings, CFG_GROUP_COLUMNS, m_groupColumns);
        settings.addBoolean(CFG_SKIP_INCOMPLETE, m_skipIncompleteRows);
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_COLUMN);
        settings.getString(CFG_ORDER_COLUMN);
        settings.getString(CFG_DIRECTION);
        settings.getInt(CFG_NUM_COPIES);
        settings.getInt(CFG_LAG_INTERVAL);
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column = settings.getString(CFG_COLUMN);
        m_orderColumn = settings.getString(CFG_ORDER_COLUMN);
        m_direction = settings.getString(CFG_DIRECTION, DIR_LAG);
        m_numCopies = settings.getInt(CFG_NUM_COPIES);
        m_lagInterval = settings.getInt(CFG_LAG_INTERVAL);
        m_groupColumns = loadColumnFilterIncluded(settings, CFG_GROUP_COLUMNS);
        m_skipIncompleteRows = settings.getBoolean(CFG_SKIP_INCOMPLETE, false);
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final List<String> included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, "STANDARD");
        sub.addStringArray(KEY_INCLUDED_NAMES, included.toArray(new String[0]));
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, "EnforceInclusion");
    }

    private static List<String> loadColumnFilterIncluded(final NodeSettingsRO settings, final String key) {
        if (!settings.containsKey(key)) {
            return List.of();
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                return Arrays.asList(sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]));
            }
            if (sub.containsKey("InclList")) {
                return Arrays.asList(sub.getStringArray("InclList", new String[0]));
            }
        } catch (final InvalidSettingsException e) {
            // ignore
        }
        return List.of();
    }
}
