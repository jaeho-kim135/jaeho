package org.knime.bigdata.spark.dx.node.preproc.cellsplit;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Cell Splitter job.
 * Contains all parameters needed to split a string column into multiple columns.
 */
@SparkClass
public class SparkCellSplitterJobInput extends JobInput {

    private static final String COLUMN = "column";
    private static final String DELIMITER = "delimiter";
    private static final String USE_REGEX = "useRegex";
    private static final String SIZE_MODE = "sizeMode";
    private static final String FIXED_SIZE = "fixedSize";
    private static final String SCAN_LIMIT = "scanLimit";
    private static final String TRIM = "trim";
    private static final String USE_EMPTY_STRING = "useEmptyString";
    private static final String REMOVE_INPUT_COL = "removeInputCol";
    private static final String OUTPUT_PREFIX = "outputPrefix";

    /** Deserialization constructor. */
    public SparkCellSplitterJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param column the column to split
     * @param delimiter the delimiter string
     * @param useRegex true if delimiter is a regex
     * @param sizeMode "FIXED" or "AUTO"
     * @param fixedSize number of output columns in FIXED mode
     * @param scanLimit row scan limit in AUTO mode
     * @param trim true to trim whitespace from split parts
     * @param useEmptyString true to use empty string instead of null
     * @param removeInputCol true to remove the input column
     * @param outputPrefix prefix for output column names
     */
    public SparkCellSplitterJobInput(final String inputObject, final String outputObject,
            final String column, final String delimiter, final boolean useRegex,
            final String sizeMode, final int fixedSize, final int scanLimit,
            final boolean trim, final boolean useEmptyString,
            final boolean removeInputCol, final String outputPrefix) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMN, column);
        set(DELIMITER, delimiter);
        set(USE_REGEX, useRegex);
        set(SIZE_MODE, sizeMode);
        set(FIXED_SIZE, fixedSize);
        set(SCAN_LIMIT, scanLimit);
        set(TRIM, trim);
        set(USE_EMPTY_STRING, useEmptyString);
        set(REMOVE_INPUT_COL, removeInputCol);
        set(OUTPUT_PREFIX, outputPrefix);
    }

    /** @return the column to split */
    public String getColumn() {
        return get(COLUMN);
    }

    /** @return the delimiter string */
    public String getDelimiter() {
        return get(DELIMITER);
    }

    /** @return true if delimiter is a regex */
    public boolean isUseRegex() {
        return get(USE_REGEX);
    }

    /** @return the size mode ("FIXED" or "AUTO") */
    public String getSizeMode() {
        return get(SIZE_MODE);
    }

    /** @return true if size mode is FIXED */
    public boolean isFixedMode() {
        return "FIXED".equals(getSizeMode());
    }

    /** @return true if size mode is AUTO */
    public boolean isAutoMode() {
        return "AUTO".equals(getSizeMode());
    }

    /** @return the fixed number of output columns */
    public int getFixedSize() {
        return get(FIXED_SIZE);
    }

    /** @return the row scan limit for auto-detect mode */
    public int getScanLimit() {
        return get(SCAN_LIMIT);
    }

    /** @return true if whitespace should be trimmed */
    public boolean isTrim() {
        return get(TRIM);
    }

    /** @return true if empty string should replace null */
    public boolean isUseEmptyString() {
        return get(USE_EMPTY_STRING);
    }

    /** @return true if the input column should be removed */
    public boolean isRemoveInputCol() {
        return get(REMOVE_INPUT_COL);
    }

    /** @return the output column prefix */
    public String getOutputPrefix() {
        return get(OUTPUT_PREFIX);
    }
}
