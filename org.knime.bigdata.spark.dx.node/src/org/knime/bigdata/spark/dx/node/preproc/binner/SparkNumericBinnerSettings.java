package org.knime.bigdata.spark.dx.node.preproc.binner;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;

/**
 * Settings for the Spark Numeric Binner node.
 *
 * <p>Stores column selection (as NameFilterConfiguration), binning mode,
 * number of bins, custom bin definitions, bin naming convention,
 * append/replace mode, and suffix.
 *
 * <p>Column filter settings use NameFilterConfiguration format
 * (filter-type / included_names / excluded_names / enforce_option) for
 * compatibility with the WebUI dialog's LegacyColumnFilterPersistor.
 */
public final class SparkNumericBinnerSettings {

    // ── Config keys ──────────────────────────────────────────────────────────
    static final String CFG_COLUMNS = "columns";
    static final String CFG_BINNING_MODE = "binningMode";
    static final String CFG_NUMBER_OF_BINS = "numberOfBins";
    static final String CFG_BIN_NAMING = "binNaming";
    static final String CFG_APPEND_OR_REPLACE = "appendOrReplace";
    static final String CFG_SUFFIX = "suffix";
    static final String CFG_CONFIGURED = "nodeConfigured";

    // Bin definition parallel arrays
    static final String CFG_BIN_NAMES = "binNames";
    static final String CFG_BIN_LEFT_BOUNDS = "binLeftBounds";
    static final String CFG_BIN_LEFT_INCLUSIVE = "binLeftInclusive";
    static final String CFG_BIN_RIGHT_BOUNDS = "binRightBounds";
    static final String CFG_BIN_RIGHT_INCLUSIVE = "binRightInclusive";

    // ── Binning mode constants ───────────────────────────────────────────────
    public static final String MODE_EQUAL_WIDTH = "EQUAL_WIDTH";
    public static final String MODE_EQUAL_FREQUENCY = "EQUAL_FREQUENCY";
    public static final String MODE_CUSTOM = "CUSTOM";

    // ── Bin naming constants ─────────────────────────────────────────────────
    public static final String NAMING_NUMBERED = "NUMBERED";
    public static final String NAMING_BORDERS = "BORDERS";
    public static final String NAMING_MIDPOINTS = "MIDPOINTS";

    // ── Append/Replace constants ─────────────────────────────────────────────
    public static final String OUTPUT_REPLACE = "REPLACE";
    public static final String OUTPUT_APPEND = "APPEND";

    /** Key used by NameFilterConfiguration for the filter type. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    // ── Fields ───────────────────────────────────────────────────────────────
    private final SettingsModelFilterString m_columns =
        new SettingsModelFilterString(CFG_COLUMNS, new String[0], new String[0], false);

    private String m_binningMode = MODE_CUSTOM;
    private int m_numberOfBins = 5;
    private String m_binNaming = NAMING_BORDERS;
    private String m_appendOrReplace = OUTPUT_REPLACE;
    private String m_suffix = "_binned";
    private boolean m_nodeConfigured = false;

    // Custom bin definitions (parallel arrays)
    private String[] m_binNames = new String[]{"Bin 1"};
    private String[] m_binLeftBounds = new String[]{"0.0"};
    private String[] m_binLeftInclusive = new String[]{"true"};
    private String[] m_binRightBounds = new String[]{"10.0"};
    private String[] m_binRightInclusive = new String[]{"false"};

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the column filter model */
    public SettingsModelFilterString getColumnsModel() {
        return m_columns;
    }

    /** @return list of selected column names */
    public List<String> getColumns() {
        return m_columns.getIncludeList();
    }

    /** @return the binning mode (EQUAL_WIDTH, EQUAL_FREQUENCY, or CUSTOM) */
    public String getBinningMode() {
        return m_binningMode;
    }

    /** @return the number of bins for auto modes */
    public int getNumberOfBins() {
        return m_numberOfBins;
    }

    /** @return the bin naming convention */
    public String getBinNaming() {
        return m_binNaming;
    }

    /** @return the append/replace mode */
    public String getAppendOrReplace() {
        return m_appendOrReplace;
    }

    /** @return whether to replace (true) or append (false) */
    public boolean isReplace() {
        return OUTPUT_REPLACE.equals(m_appendOrReplace);
    }

    /** @return the suffix for appended columns */
    public String getSuffix() {
        return m_suffix;
    }

    /** @return true if the node has been configured */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    /** @return custom bin names */
    public String[] getBinNames() {
        return m_binNames;
    }

    /** @return custom bin left bounds as strings */
    public String[] getBinLeftBounds() {
        return m_binLeftBounds;
    }

    /** @return custom bin left inclusive flags as strings */
    public String[] getBinLeftInclusive() {
        return m_binLeftInclusive;
    }

    /** @return custom bin right bounds as strings */
    public String[] getBinRightBounds() {
        return m_binRightBounds;
    }

    /** @return custom bin right inclusive flags as strings */
    public String[] getBinRightInclusive() {
        return m_binRightInclusive;
    }

    // ── Setters ──────────────────────────────────────────────────────────────

    /** @param configured set to true when user accepts the dialog */
    public void setNodeConfigured(final boolean configured) {
        m_nodeConfigured = configured;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_COLUMNS, m_columns.getIncludeList());
        settings.addString(CFG_BINNING_MODE, m_binningMode);
        settings.addInt(CFG_NUMBER_OF_BINS, m_numberOfBins);
        settings.addString(CFG_BIN_NAMING, m_binNaming);
        settings.addString(CFG_APPEND_OR_REPLACE, m_appendOrReplace);
        settings.addString(CFG_SUFFIX, m_suffix);

        // Save custom bin definitions
        settings.addStringArray(CFG_BIN_NAMES, m_binNames);
        settings.addStringArray(CFG_BIN_LEFT_BOUNDS, m_binLeftBounds);
        settings.addStringArray(CFG_BIN_LEFT_INCLUSIVE, m_binLeftInclusive);
        settings.addStringArray(CFG_BIN_RIGHT_BOUNDS, m_binRightBounds);
        settings.addStringArray(CFG_BIN_RIGHT_INCLUSIVE, m_binRightInclusive);

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
        validateColumnFilter(settings, CFG_COLUMNS);
        settings.getString(CFG_BINNING_MODE);
        settings.getInt(CFG_NUMBER_OF_BINS);
        settings.getString(CFG_BIN_NAMING);
        settings.getString(CFG_APPEND_OR_REPLACE);
        settings.getString(CFG_SUFFIX);
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_COLUMNS, m_columns);
        m_binningMode = settings.getString(CFG_BINNING_MODE);
        m_numberOfBins = settings.getInt(CFG_NUMBER_OF_BINS);
        m_binNaming = settings.getString(CFG_BIN_NAMING);
        m_appendOrReplace = settings.getString(CFG_APPEND_OR_REPLACE);
        m_suffix = settings.getString(CFG_SUFFIX);

        // Load custom bin definitions
        if (settings.containsKey(CFG_BIN_NAMES)) {
            m_binNames = settings.getStringArray(CFG_BIN_NAMES);
            m_binLeftBounds = settings.getStringArray(CFG_BIN_LEFT_BOUNDS, new String[0]);
            m_binLeftInclusive = settings.getStringArray(CFG_BIN_LEFT_INCLUSIVE, new String[0]);
            m_binRightBounds = settings.getStringArray(CFG_BIN_RIGHT_BOUNDS, new String[0]);
            m_binRightInclusive = settings.getStringArray(CFG_BIN_RIGHT_INCLUSIVE, new String[0]);
        }

        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }

    // ── Column filter format helpers ────────────────────────────────────────

    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
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
            // Fall through to old-format handler
        }
        model.loadSettingsFrom(settings);
    }

    private static void validateColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            throw new InvalidSettingsException(
                "Missing column filter configuration for key '" + key + "'.");
        }
    }
}
