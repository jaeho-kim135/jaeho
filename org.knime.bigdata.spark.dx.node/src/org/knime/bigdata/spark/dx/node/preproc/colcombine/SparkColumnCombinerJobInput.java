package org.knime.bigdata.spark.dx.node.preproc.colcombine;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Column Combiner job.
 */
@SparkClass
public class SparkColumnCombinerJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String DELIMITER = "delimiter";
    private static final String OUTPUT_COL_NAME = "outputColName";
    private static final String REMOVE_INPUT_COLS = "removeInputCols";
    private static final String HANDLE_MISSING = "handleMissing";
    private static final String QUOTE_MODE = "quoteMode";
    private static final String QUOTE_CHAR = "quoteChar";
    private static final String REPLACEMENT_DELIMITER = "replacementDelimiter";

    /** Deserialization constructor. */
    public SparkColumnCombinerJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param columns the columns to combine
     * @param delimiter the delimiter string
     * @param outputColName the output column name
     * @param removeInputCols whether to remove input columns
     * @param handleMissing the missing value handling mode (SKIP or AS_EMPTY)
     * @param quoteMode the quote mode (NONE, QUOTE, or REPLACE_IN_CELL)
     * @param quoteChar the quote character
     * @param replacementDelimiter the replacement string for delimiters in cells
     */
    public SparkColumnCombinerJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String delimiter, final String outputColName,
            final boolean removeInputCols, final String handleMissing,
            final String quoteMode, final String quoteChar, final String replacementDelimiter) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(DELIMITER, delimiter);
        set(OUTPUT_COL_NAME, outputColName);
        set(REMOVE_INPUT_COLS, removeInputCols);
        set(HANDLE_MISSING, handleMissing);
        set(QUOTE_MODE, quoteMode);
        set(QUOTE_CHAR, quoteChar);
        set(REPLACEMENT_DELIMITER, replacementDelimiter);
    }

    /** @return the columns to combine */
    public String[] getColumns() { return get(COLUMNS); }

    /** @return the delimiter string */
    public String getDelimiter() { return get(DELIMITER); }

    /** @return the output column name */
    public String getOutputColName() { return get(OUTPUT_COL_NAME); }

    /** @return whether to remove input columns */
    public boolean isRemoveInputCols() { return get(REMOVE_INPUT_COLS); }

    /** @return the missing value handling mode (SKIP or AS_EMPTY) */
    public String getHandleMissing() { return getOrDefault(HANDLE_MISSING, "SKIP"); }

    /** @return the quote mode (NONE, QUOTE, or REPLACE_IN_CELL) */
    public String getQuoteMode() { return getOrDefault(QUOTE_MODE, "NONE"); }

    /** @return the quote character */
    public String getQuoteChar() { return getOrDefault(QUOTE_CHAR, "\""); }

    /** @return the replacement string for delimiters in cells */
    public String getReplacementDelimiter() { return getOrDefault(REPLACEMENT_DELIMITER, ""); }
}
