package org.knime.bigdata.spark.dx.node.preproc.ungroup;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;

/**
 * Settings for the Spark Ungroup node.
 *
 * <p>Column filter settings are stored in NameFilterConfiguration format
 * (filter-type / included_names / excluded_names / enforce_option), which is the format
 * used by the WebUI dialog's LegacyColumnFilterPersistor. For backward compatibility,
 * loadSettingsFrom() also accepts the old SettingsModelFilterString format
 * (InclList / ExclList / keep_all_columns_selected).</p>
 */
public final class SparkUngroupSettings {

    static final String CFG_COLUMNS = "columns";
    static final String CFG_EXPLODE_MODE = "explodeMode";
    static final String CFG_DELIMITER = "delimiter";
    static final String CFG_REMOVE_ORIGINAL = "removeOriginal";
    static final String CFG_SKIP_NULLS = "skipNulls";
    static final String CFG_SKIP_EMPTY = "skipEmpty";

    /** Key used by NameFilterConfiguration for the filter type. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    /** Filter type value for manual (name-based) selection. */
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    /** Key used by NameFilterConfiguration for the included names list. */
    private static final String KEY_INCLUDED_NAMES = "included_names";
    /** Key used by NameFilterConfiguration for the excluded names list. */
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    /** Key used by NameFilterConfiguration for the enforce option. */
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    /** EnforceOption value. */
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private final SettingsModelFilterString m_columns =
        new SettingsModelFilterString(CFG_COLUMNS, new String[0], new String[0], false);

    private String m_explodeMode = "AUTO";
    private String m_delimiter = ",";
    private boolean m_removeOriginal = true;
    private boolean m_skipNulls = false;
    private boolean m_skipEmpty = false;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return list of selected column names */
    public List<String> getColumns() {
        return m_columns.getIncludeList();
    }

    /** @return the columns filter model */
    public SettingsModelFilterString getColumnsModel() {
        return m_columns;
    }

    /** @return the explode mode as string (AUTO or STRING_SPLIT) */
    public String getExplodeMode() {
        return m_explodeMode;
    }

    /** @return the delimiter for string splitting */
    public String getDelimiter() {
        return m_delimiter;
    }

    /** @return whether to remove the original column after explode */
    public boolean isRemoveOriginal() {
        return m_removeOriginal;
    }

    /** @return whether to skip null rows */
    public boolean isSkipNulls() {
        return m_skipNulls;
    }

    /** @return whether to skip empty collections/strings */
    public boolean isSkipEmpty() {
        return m_skipEmpty;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     * Column filters are written in NameFilterConfiguration format (compatible with WebUI dialog).
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_COLUMNS, m_columns.getIncludeList());
        settings.addString(CFG_EXPLODE_MODE, m_explodeMode);
        settings.addString(CFG_DELIMITER, m_delimiter);
        settings.addBoolean(CFG_REMOVE_ORIGINAL, m_removeOriginal);
        settings.addBoolean(CFG_SKIP_NULLS, m_skipNulls);
        settings.addBoolean(CFG_SKIP_EMPTY, m_skipEmpty);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateColumnFilter(settings, CFG_COLUMNS);
        if (settings.containsKey(CFG_EXPLODE_MODE)) {
            settings.getString(CFG_EXPLODE_MODE);
        }
        if (settings.containsKey(CFG_DELIMITER)) {
            settings.getString(CFG_DELIMITER);
        }
        if (settings.containsKey(CFG_REMOVE_ORIGINAL)) {
            settings.getBoolean(CFG_REMOVE_ORIGINAL);
        }
        if (settings.containsKey(CFG_SKIP_NULLS)) {
            settings.getBoolean(CFG_SKIP_NULLS);
        }
        if (settings.containsKey(CFG_SKIP_EMPTY)) {
            settings.getBoolean(CFG_SKIP_EMPTY);
        }
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_COLUMNS, m_columns);
        m_explodeMode = settings.getString(CFG_EXPLODE_MODE, "AUTO");
        m_delimiter = settings.getString(CFG_DELIMITER, ",");
        m_removeOriginal = settings.containsKey(CFG_REMOVE_ORIGINAL)
            ? settings.getBoolean(CFG_REMOVE_ORIGINAL) : true;
        m_skipNulls = settings.containsKey(CFG_SKIP_NULLS)
            ? settings.getBoolean(CFG_SKIP_NULLS) : false;
        m_skipEmpty = settings.containsKey(CFG_SKIP_EMPTY)
            ? settings.getBoolean(CFG_SKIP_EMPTY) : false;
    }

    // ── Column filter format helpers ──────────────────────────────────────────

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
     * Loads a column filter, handling both new and old formats.
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
            // Fall through to old format
        }
        model.loadSettingsFrom(settings);
    }

    /**
     * Validates a column filter entry.
     */
    private static void validateColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            throw new InvalidSettingsException(
                "Missing column filter configuration for key '" + key + "'.");
        }
    }
}
