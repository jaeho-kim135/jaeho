package org.knime.bigdata.spark.dx.node.preproc.colcombine;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Column Combiner node.
 *
 * <p>Column filter settings are stored in NameFilterConfiguration format
 * (filter-type / included_names / excluded_names / enforce_option), which is the format
 * used by the WebUI dialog's LegacyColumnFilterPersistor. For backward compatibility,
 * loadSettingsFrom() also accepts the old SettingsModelFilterString format
 * (InclList / ExclList / keep_all_columns_selected).</p>
 */
public final class SparkColumnCombinerSettings {

    static final String CFG_COLUMNS = "columns";
    static final String CFG_DELIMITER = "delimiter";
    static final String CFG_OUTPUT_COL_NAME = "outputColName";
    static final String CFG_REMOVE_INPUT_COLS = "removeInputCols";
    static final String CFG_HANDLE_MISSING = "handleMissing";
    static final String CFG_QUOTE_MODE = "quoteMode";
    static final String CFG_QUOTE_CHAR = "quoteChar";
    static final String CFG_REPLACEMENT_DELIMITER = "replacementDelimiter";

    /** MissingHandling constants. */
    public static final String MISSING_SKIP = "SKIP";
    public static final String MISSING_AS_EMPTY = "AS_EMPTY";

    /** QuoteMode constants. */
    public static final String QUOTE_NONE = "NONE";
    public static final String QUOTE_QUOTE = "QUOTE";
    public static final String QUOTE_REPLACE_IN_CELL = "REPLACE_IN_CELL";

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
    /** EnforceOption value meaning "exclude unknown columns". */
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private final SettingsModelFilterString m_columns =
        new SettingsModelFilterString(CFG_COLUMNS, new String[0], new String[0], false);

    private final SettingsModelString m_delimiter =
        new SettingsModelString(CFG_DELIMITER, ",");

    private final SettingsModelString m_outputColName =
        new SettingsModelString(CFG_OUTPUT_COL_NAME, "Combined");

    private final SettingsModelBoolean m_removeInputCols =
        new SettingsModelBoolean(CFG_REMOVE_INPUT_COLS, false);

    private final SettingsModelString m_quoteChar =
        new SettingsModelString(CFG_QUOTE_CHAR, "\"");

    private final SettingsModelString m_replacementDelimiter =
        new SettingsModelString(CFG_REPLACEMENT_DELIMITER, "");

    private String m_handleMissing = MISSING_SKIP;
    private String m_quoteMode = QUOTE_NONE;

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return list of selected column names */
    public List<String> getColumns() {
        return m_columns.getIncludeList();
    }

    /** @return the delimiter string */
    public String getDelimiter() {
        return m_delimiter.getStringValue();
    }

    /** @return the output column name */
    public String getOutputColName() {
        return m_outputColName.getStringValue();
    }

    /** @return whether to remove input columns after combining */
    public boolean removeInputCols() {
        return m_removeInputCols.getBooleanValue();
    }

    /** @return the missing value handling mode */
    public String getHandleMissing() {
        return m_handleMissing;
    }

    /** @return the quote mode */
    public String getQuoteMode() {
        return m_quoteMode;
    }

    /** @return the quote character */
    public String getQuoteChar() {
        return m_quoteChar.getStringValue();
    }

    /** @return the replacement delimiter string */
    public String getReplacementDelimiter() {
        return m_replacementDelimiter.getStringValue();
    }

    // ── Save / Validate / Load ───────────────────────────────────────────────

    /**
     * Save settings.
     * Column filter is written in NameFilterConfiguration format (compatible with WebUI dialog).
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_COLUMNS, m_columns.getIncludeList());
        m_delimiter.saveSettingsTo(settings);
        m_outputColName.saveSettingsTo(settings);
        m_removeInputCols.saveSettingsTo(settings);
        settings.addString(CFG_HANDLE_MISSING, m_handleMissing);
        settings.addString(CFG_QUOTE_MODE, m_quoteMode);
        m_quoteChar.saveSettingsTo(settings);
        m_replacementDelimiter.saveSettingsTo(settings);
    }

    /**
     * Validate settings.
     * Accepts both NameFilterConfiguration format (new) and SettingsModelFilterString format (old).
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateColumnFilter(settings, CFG_COLUMNS);
        m_delimiter.validateSettings(settings);
        m_outputColName.validateSettings(settings);
        m_removeInputCols.validateSettings(settings);
    }

    /**
     * Load validated settings.
     * Handles both NameFilterConfiguration format (new) and SettingsModelFilterString format (old).
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_COLUMNS, m_columns);
        m_delimiter.loadSettingsFrom(settings);
        m_outputColName.loadSettingsFrom(settings);
        m_removeInputCols.loadSettingsFrom(settings);
        m_handleMissing = settings.getString(CFG_HANDLE_MISSING, MISSING_SKIP);
        m_quoteMode = settings.getString(CFG_QUOTE_MODE, QUOTE_NONE);
        if (settings.containsKey(CFG_QUOTE_CHAR)) {
            m_quoteChar.loadSettingsFrom(settings);
        }
        if (settings.containsKey(CFG_REPLACEMENT_DELIMITER)) {
            m_replacementDelimiter.loadSettingsFrom(settings);
        }
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
                // New format: NameFilterConfiguration / LegacyColumnFilterPersistor
                final String[] incl = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                model.setIncludeList(Arrays.asList(incl));
                return;
            }
        } catch (final InvalidSettingsException e) {
            // Sub-config is Config type (old format) — fall through
        }
        // Old format: SettingsModelFilterString
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
