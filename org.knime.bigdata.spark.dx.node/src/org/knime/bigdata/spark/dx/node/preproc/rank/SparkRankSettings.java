package org.knime.bigdata.spark.dx.node.preproc.rank;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Rank node.
 *
 * <p>Stores ranking columns/orders as parallel String arrays, group columns in
 * NameFilterConfiguration format (for compatibility with LegacyColumnFilterPersistor),
 * and scalar options for rank mode, output column name, data type, and missing handling.</p>
 */
public final class SparkRankSettings {

    // ── Config keys ──────────────────────────────────────────────────────────
    static final String CFG_RANKING_COLUMNS = "rankingColumns";
    static final String CFG_RANKING_ORDERS = "rankingOrders";
    static final String CFG_GROUP_COLUMNS = "groupColumns";
    static final String CFG_RANK_MODE = "rankMode";
    static final String CFG_OUTPUT_COL_NAME = "outputColName";
    static final String CFG_RANK_DATA_TYPE = "rankDataType";
    static final String CFG_MISSING_TO_END = "missingToEnd";
    static final String CFG_CONFIGURED = "nodeConfigured";

    /** NameFilterConfiguration keys for group columns. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    // ── Fields ────────────────────────────────────────────────────────────────
    private String[] m_rankingColumns = new String[0];
    private String[] m_rankingOrders = new String[0];

    private final SettingsModelFilterString m_groupColumns =
        new SettingsModelFilterString(CFG_GROUP_COLUMNS, new String[0], new String[0], false);

    private final SettingsModelString m_rankMode =
        new SettingsModelString(CFG_RANK_MODE, "STANDARD");

    private final SettingsModelString m_outputColName =
        new SettingsModelString(CFG_OUTPUT_COL_NAME, "Rank");

    private final SettingsModelString m_rankDataType =
        new SettingsModelString(CFG_RANK_DATA_TYPE, "LONG");

    private final SettingsModelBoolean m_missingToEnd =
        new SettingsModelBoolean(CFG_MISSING_TO_END, true);

    private boolean m_nodeConfigured = false;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the ranking column names */
    public String[] getRankingColumns() { return m_rankingColumns; }

    /** @return the ranking orders (ASCENDING/DESCENDING per column) */
    public String[] getRankingOrders() { return m_rankingOrders; }

    /** @return the group column names */
    public List<String> getGroupColumns() { return m_groupColumns.getIncludeList(); }

    /** @return the rank mode string (STANDARD/DENSE/ORDINAL) */
    public String getRankMode() { return m_rankMode.getStringValue(); }

    /** @return the output rank column name */
    public String getOutputColName() { return m_outputColName.getStringValue(); }

    /** @return the rank data type (INTEGER/LONG) */
    public String getRankDataType() { return m_rankDataType.getStringValue(); }

    /** @return whether to sort missing values to the end */
    public boolean isMissingToEnd() { return m_missingToEnd.getBooleanValue(); }

    /** @return true if the node has been configured */
    public boolean isNodeConfigured() { return m_nodeConfigured; }

    // ── Save / Validate / Load ───────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addStringArray(CFG_RANKING_COLUMNS, m_rankingColumns);
        settings.addStringArray(CFG_RANKING_ORDERS, m_rankingOrders);
        writeColumnFilter(settings, CFG_GROUP_COLUMNS, m_groupColumns.getIncludeList());
        m_rankMode.saveSettingsTo(settings);
        m_outputColName.saveSettingsTo(settings);
        m_rankDataType.saveSettingsTo(settings);
        m_missingToEnd.saveSettingsTo(settings);
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
        if (!settings.containsKey(CFG_RANKING_COLUMNS)) {
            throw new InvalidSettingsException("Missing ranking columns configuration.");
        }
        m_rankMode.validateSettings(settings);
        m_outputColName.validateSettings(settings);
        if (settings.containsKey(CFG_RANK_DATA_TYPE)) {
            m_rankDataType.validateSettings(settings);
        }
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_rankingColumns = settings.getStringArray(CFG_RANKING_COLUMNS);
        m_rankingOrders = settings.getStringArray(CFG_RANKING_ORDERS, new String[0]);
        loadColumnFilter(settings, CFG_GROUP_COLUMNS, m_groupColumns);
        m_rankMode.loadSettingsFrom(settings);
        m_outputColName.loadSettingsFrom(settings);
        if (settings.containsKey(CFG_RANK_DATA_TYPE)) {
            m_rankDataType.loadSettingsFrom(settings);
        }
        if (settings.containsKey(CFG_MISSING_TO_END)) {
            m_missingToEnd.loadSettingsFrom(settings);
        }
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    // ── Column filter helpers ────────────────────────────────────────────────

    /**
     * Writes a column filter in NameFilterConfiguration format.
     */
    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final List<String> included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, included.toArray(new String[0]));
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);
    }

    /**
     * Loads a column filter, handling both NameFilterConfiguration and
     * SettingsModelFilterString formats.
     */
    private static void loadColumnFilter(final NodeSettingsRO settings, final String key,
            final SettingsModelFilterString model) throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            return;
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                final String[] incl = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                model.setIncludeList(Arrays.asList(incl));
                return;
            }
        } catch (final InvalidSettingsException e) {
            // fall through
        }
        model.loadSettingsFrom(settings);
    }
}
