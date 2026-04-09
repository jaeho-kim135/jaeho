package org.knime.bigdata.spark.dx.node.preproc.topk;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;

/**
 * Settings for the Spark Top k Row Filter node.
 *
 * <p>Stores sort columns/orders as parallel arrays, k value, filter mode,
 * output order, missing handling, and group columns.</p>
 */
public final class SparkTopKRowFilterSettings {

    // ── Config keys (new array format) ──────────────────────────────────────
    static final String CFG_K = "k";
    static final String CFG_FILTER_MODE = "filterMode";
    static final String CFG_OUTPUT_ORDER = "outputOrder";
    static final String CFG_MISSINGS_TO_END = "missingsToEnd";
    static final String CFG_GROUP_COLUMNS = "groupColumns";
    static final String CFG_SORT_COLUMNS = "sortColumns";
    static final String CFG_SORT_ORDERS = "sortOrders";

    // ── Legacy config keys (for backward compat loading) ────────────────────
    static final String CFG_SORT_COLUMN1_LEGACY = "sortColumn1";
    static final String CFG_SORT_ORDER1_LEGACY = "sortOrder1";
    static final String CFG_SORT_COLUMN2_LEGACY = "sortColumn2";
    static final String CFG_SORT_ORDER2_LEGACY = "sortOrder2";
    static final String CFG_USE_SECOND_SORT_LEGACY = "useSecondSort";

    /** NameFilterConfiguration keys for group columns. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    // ── Fields ────────────────────────────────────────────────────────────────
    private long m_k = 5;
    private String m_filterMode = "ROWS";
    private String m_outputOrder = "SORTED";
    private boolean m_missingsToEnd = true;
    private String[] m_sortColumns = new String[]{""};
    private String[] m_sortOrders = new String[]{"DESCENDING"};

    private final SettingsModelFilterString m_groupColumns =
        new SettingsModelFilterString(CFG_GROUP_COLUMNS, new String[0], new String[0], false);

    // ── Getters ──────────────────────────────────────────────────────────────

    public long getK() { return m_k; }
    public String getFilterMode() { return m_filterMode; }
    public String getOutputOrder() { return m_outputOrder; }
    public boolean isMissingsToEnd() { return m_missingsToEnd; }
    public String[] getSortColumns() { return m_sortColumns; }
    public String[] getSortOrders() { return m_sortOrders; }
    public List<String> getGroupColumns() { return m_groupColumns.getIncludeList(); }

    // ── Save / Validate / Load ───────────────────────────────────────────────

    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addLong(CFG_K, m_k);
        settings.addString(CFG_FILTER_MODE, m_filterMode);
        settings.addString(CFG_OUTPUT_ORDER, m_outputOrder);
        settings.addBoolean(CFG_MISSINGS_TO_END, m_missingsToEnd);
        settings.addStringArray(CFG_SORT_COLUMNS, m_sortColumns);
        settings.addStringArray(CFG_SORT_ORDERS, m_sortOrders);
        writeColumnFilter(settings, CFG_GROUP_COLUMNS, m_groupColumns.getIncludeList());
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_K)) {
            throw new InvalidSettingsException("Missing 'k' setting.");
        }
        // Accept either new or legacy format
        if (!settings.containsKey(CFG_SORT_COLUMNS) && !settings.containsKey(CFG_SORT_COLUMN1_LEGACY)) {
            throw new InvalidSettingsException("Missing sort criteria settings.");
        }
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_k = settings.getLong(CFG_K);
        m_filterMode = settings.getString(CFG_FILTER_MODE, "ROWS");
        m_outputOrder = settings.getString(CFG_OUTPUT_ORDER, "SORTED");
        m_missingsToEnd = settings.getBoolean(CFG_MISSINGS_TO_END, true);

        // New format
        if (settings.containsKey(CFG_SORT_COLUMNS)) {
            m_sortColumns = settings.getStringArray(CFG_SORT_COLUMNS);
            m_sortOrders = settings.getStringArray(CFG_SORT_ORDERS,
                new String[m_sortColumns.length]);
            // Fill missing orders with default
            for (int i = 0; i < m_sortOrders.length; i++) {
                if (m_sortOrders[i] == null) {
                    m_sortOrders[i] = "DESCENDING";
                }
            }
        } else if (settings.containsKey(CFG_SORT_COLUMN1_LEGACY)) {
            // Backward compat: convert old format to arrays
            final String col1 = settings.getString(CFG_SORT_COLUMN1_LEGACY);
            final String ord1 = settings.getString(CFG_SORT_ORDER1_LEGACY, "DESCENDING");
            final boolean useSecond = settings.getBoolean(CFG_USE_SECOND_SORT_LEGACY, false);

            if (useSecond) {
                final String col2 = settings.getString(CFG_SORT_COLUMN2_LEGACY, "");
                final String ord2 = settings.getString(CFG_SORT_ORDER2_LEGACY, "DESCENDING");
                if (!col2.isEmpty()) {
                    m_sortColumns = new String[]{col1, col2};
                    m_sortOrders = new String[]{ord1, ord2};
                } else {
                    m_sortColumns = new String[]{col1};
                    m_sortOrders = new String[]{ord1};
                }
            } else {
                m_sortColumns = new String[]{col1};
                m_sortOrders = new String[]{ord1};
            }
        }

        loadColumnFilter(settings, CFG_GROUP_COLUMNS, m_groupColumns);
    }

    // ── Column filter helpers ────────────────────────────────────────────────

    static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final List<String> included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, included.toArray(new String[0]));
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);
    }

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
