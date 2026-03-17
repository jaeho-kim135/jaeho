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
 * <p>Stores sort columns/orders, k value, filter mode, output order, missing handling,
 * and group columns. Column filters use NameFilterConfiguration format for compatibility
 * with the WebUI dialog's LegacyColumnFilterPersistor.</p>
 */
public final class SparkTopKRowFilterSettings {

    // ── Config keys ──────────────────────────────────────────────────────────
    static final String CFG_K = "k";
    static final String CFG_FILTER_MODE = "filterMode";
    static final String CFG_OUTPUT_ORDER = "outputOrder";
    static final String CFG_MISSINGS_TO_END = "missingsToEnd";
    static final String CFG_GROUP_COLUMNS = "groupColumns";
    static final String CFG_SORT_COLUMN1 = "sortColumn1";
    static final String CFG_SORT_ORDER1 = "sortOrder1";
    static final String CFG_SORT_COLUMN2 = "sortColumn2";
    static final String CFG_SORT_ORDER2 = "sortOrder2";
    static final String CFG_USE_SECOND_SORT = "useSecondSort";

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
    private String m_sortColumn1 = "";
    private String m_sortOrder1 = "DESCENDING";
    private String m_sortColumn2 = "";
    private String m_sortOrder2 = "DESCENDING";
    private boolean m_useSecondSort = false;

    private final SettingsModelFilterString m_groupColumns =
        new SettingsModelFilterString(CFG_GROUP_COLUMNS, new String[0], new String[0], false);

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the number of rows to select */
    public long getK() { return m_k; }

    /** @return the filter mode string (ROWS or UNIQUE_VALUES) */
    public String getFilterMode() { return m_filterMode; }

    /** @return the output order string (SORTED or ARBITRARY) */
    public String getOutputOrder() { return m_outputOrder; }

    /** @return whether to put null values at end of sort */
    public boolean isMissingsToEnd() { return m_missingsToEnd; }

    /** @return the primary sort column name */
    public String getSortColumn1() { return m_sortColumn1; }

    /** @return the primary sort order (ASCENDING/DESCENDING) */
    public String getSortOrder1() { return m_sortOrder1; }

    /** @return the secondary sort column name */
    public String getSortColumn2() { return m_sortColumn2; }

    /** @return the secondary sort order (ASCENDING/DESCENDING) */
    public String getSortOrder2() { return m_sortOrder2; }

    /** @return whether to use a second sort criterion */
    public boolean useSecondSort() { return m_useSecondSort; }

    /** @return the group column names */
    public List<String> getGroupColumns() { return m_groupColumns.getIncludeList(); }

    // ── Save / Validate / Load ───────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addLong(CFG_K, m_k);
        settings.addString(CFG_FILTER_MODE, m_filterMode);
        settings.addString(CFG_OUTPUT_ORDER, m_outputOrder);
        settings.addBoolean(CFG_MISSINGS_TO_END, m_missingsToEnd);
        settings.addString(CFG_SORT_COLUMN1, m_sortColumn1);
        settings.addString(CFG_SORT_ORDER1, m_sortOrder1);
        settings.addString(CFG_SORT_COLUMN2, m_sortColumn2);
        settings.addString(CFG_SORT_ORDER2, m_sortOrder2);
        settings.addBoolean(CFG_USE_SECOND_SORT, m_useSecondSort);
        writeColumnFilter(settings, CFG_GROUP_COLUMNS, m_groupColumns.getIncludeList());
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_K)) {
            throw new InvalidSettingsException("Missing 'k' setting.");
        }
        if (!settings.containsKey(CFG_SORT_COLUMN1)) {
            throw new InvalidSettingsException("Missing primary sort column setting.");
        }
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_k = settings.getLong(CFG_K);
        m_filterMode = settings.getString(CFG_FILTER_MODE, "ROWS");
        m_outputOrder = settings.getString(CFG_OUTPUT_ORDER, "SORTED");
        m_missingsToEnd = settings.getBoolean(CFG_MISSINGS_TO_END, true);
        m_sortColumn1 = settings.getString(CFG_SORT_COLUMN1);
        m_sortOrder1 = settings.getString(CFG_SORT_ORDER1, "DESCENDING");
        m_sortColumn2 = settings.getString(CFG_SORT_COLUMN2, "");
        m_sortOrder2 = settings.getString(CFG_SORT_ORDER2, "DESCENDING");
        m_useSecondSort = settings.getBoolean(CFG_USE_SECOND_SORT, false);
        loadColumnFilter(settings, CFG_GROUP_COLUMNS, m_groupColumns);
    }

    // ── Column filter helpers ────────────────────────────────────────────────

    /**
     * Writes a column filter in NameFilterConfiguration format.
     */
    static void writeColumnFilter(final NodeSettingsWO settings, final String key,
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
