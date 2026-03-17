package org.knime.bigdata.spark.dx.node.sql.multiquery;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Multi Query node.
 *
 * <p>Column filter settings are stored in NameFilterConfiguration format
 * (filter-type / included_names / excluded_names / enforce_option), which is the format
 * used by the WebUI dialog's LegacyColumnFilterPersistor. For backward compatibility,
 * loadSettingsFrom() also accepts the old SettingsModelFilterString format
 * (InclList / ExclList / keep_all_columns_selected).</p>
 */
public final class SparkMultiQuerySettings {

    /** Placeholder token replaced with each target column name at execution time. */
    public static final String COLUMN_PLACEHOLDER = "$columnS";

    static final String CFG_TARGET_COLUMNS = "targetColumns";
    static final String CFG_SQL_EXPRESSION = "sqlExpression";
    static final String CFG_KEEP_ORIGINAL = "keepOriginalColumns";
    static final String CFG_OUTPUT_PATTERN = "outputColumnPattern";
    static final String CFG_CONFIGURED = "nodeConfigured";

    /** Key used by NameFilterConfiguration / LegacyColumnFilterPersistor for the filter type. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    /** Filter type value for manual (name-based) selection. */
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    /** Key used by NameFilterConfiguration for the included names list. */
    private static final String KEY_INCLUDED_NAMES = "included_names";
    /** Key used by NameFilterConfiguration for the excluded names list. */
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    /** Key used by NameFilterConfiguration for the enforce option. */
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    /** EnforceOption value meaning "include list is authoritative". */
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private final SettingsModelFilterString m_targetColumns =
        new SettingsModelFilterString(CFG_TARGET_COLUMNS, new String[0], new String[0], false);

    private final SettingsModelString m_sqlExpression =
        new SettingsModelString(CFG_SQL_EXPRESSION, "string(" + COLUMN_PLACEHOLDER + ")");

    private final SettingsModelBoolean m_keepOriginal =
        new SettingsModelBoolean(CFG_KEEP_ORIGINAL, false);

    private final SettingsModelString m_outputPattern =
        new SettingsModelString(CFG_OUTPUT_PATTERN, COLUMN_PLACEHOLDER);

    /** True once the user has accepted the dialog settings with OK at least once. */
    private boolean m_nodeConfigured = false;

    /** @return true if the node has been configured (user clicked OK at least once) */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    /** @param configured set to true when user accepts the dialog */
    public void setNodeConfigured(final boolean configured) {
        m_nodeConfigured = configured;
    }

    /** @return the target columns filter model */
    public SettingsModelFilterString getTargetColumnsModel() {
        return m_targetColumns;
    }

    /** @return the SQL expression model */
    public SettingsModelString getSqlExpressionModel() {
        return m_sqlExpression;
    }

    /** @return the keep original columns model */
    public SettingsModelBoolean getKeepOriginalModel() {
        return m_keepOriginal;
    }

    /** @return the output column pattern model */
    public SettingsModelString getOutputPatternModel() {
        return m_outputPattern;
    }

    /** @return list of target column names */
    public List<String> getTargetColumns() {
        return m_targetColumns.getIncludeList();
    }

    /** @return the SQL expression template */
    public String getSqlExpression() {
        return m_sqlExpression.getStringValue();
    }

    /** @return whether to keep original columns alongside transformed ones */
    public boolean keepOriginalColumns() {
        return m_keepOriginal.getBooleanValue();
    }

    /** @return the output column name pattern (uses $columnS placeholder) */
    public String getOutputColumnPattern() {
        return m_outputPattern.getStringValue();
    }

    /**
     * Save settings.
     * Column filters are written in NameFilterConfiguration format (compatible with WebUI dialog).
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_TARGET_COLUMNS, m_targetColumns.getIncludeList());
        m_sqlExpression.saveSettingsTo(settings);
        m_keepOriginal.saveSettingsTo(settings);
        m_outputPattern.saveSettingsTo(settings);
        // Only written once the user has accepted the dialog with OK; omitted for fresh nodes
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     * Accepts both NameFilterConfiguration format (new) and SettingsModelFilterString format (old).
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateColumnFilter(settings, CFG_TARGET_COLUMNS);
        m_sqlExpression.validateSettings(settings);
        if (settings.containsKey(CFG_KEEP_ORIGINAL)) {
            m_keepOriginal.validateSettings(settings);
        }
        if (settings.containsKey(CFG_OUTPUT_PATTERN)) {
            m_outputPattern.validateSettings(settings);
        }
    }

    /**
     * Load validated settings.
     * Handles both NameFilterConfiguration format (new, written by WebUI dialog) and
     * SettingsModelFilterString format (old, for backward compatibility with saved workflows).
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_TARGET_COLUMNS, m_targetColumns);
        m_sqlExpression.loadSettingsFrom(settings);
        if (settings.containsKey(CFG_KEEP_ORIGINAL)) {
            m_keepOriginal.loadSettingsFrom(settings);
        }
        if (settings.containsKey(CFG_OUTPUT_PATTERN)) {
            m_outputPattern.loadSettingsFrom(settings);
        }
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    // ── Column filter format helpers ──────────────────────────────────────────

    /**
     * Writes a column filter in NameFilterConfiguration format (filter-type / included_names /
     * excluded_names / enforce_option). This format is compatible with LegacyColumnFilterPersistor
     * used by the WebUI dialog.
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
     * Loads a column filter, handling both formats:
     * <ul>
     *   <li>New (NameFilterConfiguration): sub-config contains {@code included_names}</li>
     *   <li>Old (SettingsModelFilterString): sub-config contains {@code InclList}</li>
     * </ul>
     * If the key is absent, the model is left at its default value.
     */
    private static void loadColumnFilter(final NodeSettingsRO settings, final String key,
            final SettingsModelFilterString model) throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            return;
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                // New format: NameFilterConfiguration / LegacyColumnFilterPersistor
                final String[] incl = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                model.setIncludeList(Arrays.asList(incl));
                return;
            }
        } catch (final InvalidSettingsException e) {
            // Sub-config is Config type (old format) — fall through to old-format handler
        }
        // Old format: SettingsModelFilterString (InclList / ExclList / keep_all_columns_selected)
        model.loadSettingsFrom(settings);
    }

    /**
     * Validates a column filter entry. Accepts both new (NameFilterConfiguration) and
     * old (SettingsModelFilterString) formats. Simply checks the key is present.
     */
    private static void validateColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            throw new InvalidSettingsException(
                "Missing column filter configuration for key '" + key + "'.");
        }
        // Both formats are acceptable — detailed content validation is left to loadSettingsFrom()
    }
}
