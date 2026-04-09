package org.knime.bigdata.spark.dx.node.preproc.cellsplit;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Cell Splitter node.
 * Stores column, delimiter, size mode, and output options.
 */
public final class SparkCellSplitterSettings {

    static final String CFG_COLUMN = "column";
    static final String CFG_DELIMITER = "delimiter";
    static final String CFG_USE_REGEX = "useRegex";
    static final String CFG_SIZE_MODE = "sizeMode";
    static final String CFG_FIXED_SIZE = "fixedSize";
    static final String CFG_SCAN_LIMIT = "scanLimit";
    static final String CFG_TRIM = "trim";
    static final String CFG_USE_EMPTY_STRING = "useEmptyString";
    static final String CFG_REMOVE_INPUT_COL = "removeInputCol";
    static final String CFG_OUTPUT_PREFIX = "outputPrefix";

    private String m_column = "";
    private String m_delimiter = ",";
    private boolean m_useRegex = false;
    private String m_sizeMode = "FIXED";
    private int m_fixedSize = 3;
    private int m_scanLimit = 10000;
    private boolean m_trim = true;
    private boolean m_useEmptyString = false;
    private boolean m_removeInputCol = false;
    private String m_outputPrefix = "";

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the target column name */
    public String getColumn() {
        return m_column;
    }

    /** @return the delimiter string */
    public String getDelimiter() {
        return m_delimiter;
    }

    /** @return true if the delimiter is a regular expression */
    public boolean isUseRegex() {
        return m_useRegex;
    }

    /** @return the size mode as string ("FIXED" or "AUTO") */
    public String getSizeMode() {
        return m_sizeMode;
    }

    /** @return true if size mode is FIXED */
    public boolean isFixedMode() {
        return "FIXED".equals(m_sizeMode);
    }

    /** @return true if size mode is AUTO */
    public boolean isAutoMode() {
        return "AUTO".equals(m_sizeMode);
    }

    /** @return the fixed number of output columns */
    public int getFixedSize() {
        return m_fixedSize;
    }

    /** @return the row scan limit for auto-detect mode */
    public int getScanLimit() {
        return m_scanLimit;
    }

    /** @return true if whitespace should be trimmed from split parts */
    public boolean isTrim() {
        return m_trim;
    }

    /** @return true if missing values should be replaced with empty strings */
    public boolean isUseEmptyString() {
        return m_useEmptyString;
    }

    /** @return true if the input column should be removed */
    public boolean isRemoveInputCol() {
        return m_removeInputCol;
    }

    /** @return the output column name prefix (empty means use input column name) */
    public String getOutputPrefix() {
        return m_outputPrefix;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     *
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_COLUMN, m_column);
        settings.addString(CFG_DELIMITER, m_delimiter);
        settings.addBoolean(CFG_USE_REGEX, m_useRegex);
        settings.addString(CFG_SIZE_MODE, m_sizeMode);
        settings.addInt(CFG_FIXED_SIZE, m_fixedSize);
        settings.addInt(CFG_SCAN_LIMIT, m_scanLimit);
        settings.addBoolean(CFG_TRIM, m_trim);
        settings.addBoolean(CFG_USE_EMPTY_STRING, m_useEmptyString);
        settings.addBoolean(CFG_REMOVE_INPUT_COL, m_removeInputCol);
        settings.addString(CFG_OUTPUT_PREFIX, m_outputPrefix);
    }

    /**
     * Validate settings.
     *
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_COLUMN);
        settings.getString(CFG_DELIMITER);
        settings.getBoolean(CFG_USE_REGEX);
        settings.getString(CFG_SIZE_MODE);
        settings.getInt(CFG_FIXED_SIZE);
        settings.getInt(CFG_SCAN_LIMIT);
        settings.getBoolean(CFG_TRIM);
        settings.getBoolean(CFG_USE_EMPTY_STRING);
        settings.getBoolean(CFG_REMOVE_INPUT_COL);
        settings.getString(CFG_OUTPUT_PREFIX);
    }

    /**
     * Load validated settings.
     *
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column = settings.getString(CFG_COLUMN);
        m_delimiter = settings.getString(CFG_DELIMITER);
        m_useRegex = settings.getBoolean(CFG_USE_REGEX);
        m_sizeMode = settings.getString(CFG_SIZE_MODE);
        m_fixedSize = settings.getInt(CFG_FIXED_SIZE);
        m_scanLimit = settings.getInt(CFG_SCAN_LIMIT);
        m_trim = settings.getBoolean(CFG_TRIM);
        m_useEmptyString = settings.getBoolean(CFG_USE_EMPTY_STRING);
        m_removeInputCol = settings.getBoolean(CFG_REMOVE_INPUT_COL);
        m_outputPrefix = settings.getString(CFG_OUTPUT_PREFIX);
    }
}
