package org.knime.bigdata.spark.dx.node.preproc.binner;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Numeric Binner job.
 * Contains column names, binning mode, bin definitions, and output options.
 */
@SparkClass
public class SparkNumericBinnerJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String BINNING_MODE = "binningMode";
    private static final String NUMBER_OF_BINS = "numberOfBins";
    private static final String BIN_NAMING = "binNaming";
    private static final String REPLACE = "replace";
    private static final String SUFFIX = "suffix";

    // Custom bin definitions (parallel arrays)
    private static final String BIN_NAMES = "binNames";
    private static final String BIN_LEFT_BOUNDS = "binLeftBounds";
    private static final String BIN_LEFT_INCLUSIVE = "binLeftInclusive";
    private static final String BIN_RIGHT_BOUNDS = "binRightBounds";
    private static final String BIN_RIGHT_INCLUSIVE = "binRightInclusive";

    /** Deserialization constructor. */
    public SparkNumericBinnerJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param columns columns to bin
     * @param binningMode binning mode (EQUAL_WIDTH, EQUAL_FREQUENCY, CUSTOM)
     * @param numberOfBins number of bins for auto modes
     * @param binNaming bin naming convention (NUMBERED, BORDERS, MIDPOINTS)
     * @param replace true to replace columns, false to append
     * @param suffix suffix for appended columns
     * @param binNames custom bin names
     * @param binLeftBounds custom left boundary values
     * @param binLeftInclusive custom left inclusive flags
     * @param binRightBounds custom right boundary values
     * @param binRightInclusive custom right inclusive flags
     */
    public SparkNumericBinnerJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String binningMode,
            final int numberOfBins, final String binNaming,
            final boolean replace, final String suffix,
            final String[] binNames, final String[] binLeftBounds,
            final String[] binLeftInclusive, final String[] binRightBounds,
            final String[] binRightInclusive) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(BINNING_MODE, binningMode);
        set(NUMBER_OF_BINS, numberOfBins);
        set(BIN_NAMING, binNaming);
        set(REPLACE, replace);
        set(SUFFIX, suffix);
        set(BIN_NAMES, binNames);
        set(BIN_LEFT_BOUNDS, binLeftBounds);
        set(BIN_LEFT_INCLUSIVE, binLeftInclusive);
        set(BIN_RIGHT_BOUNDS, binRightBounds);
        set(BIN_RIGHT_INCLUSIVE, binRightInclusive);
    }

    /** @return columns to bin */
    public String[] getColumns() {
        return get(COLUMNS);
    }

    /** @return binning mode */
    public String getBinningMode() {
        return get(BINNING_MODE);
    }

    /** @return number of bins for auto modes */
    public int getNumberOfBins() {
        return get(NUMBER_OF_BINS);
    }

    /** @return bin naming convention */
    public String getBinNaming() {
        return get(BIN_NAMING);
    }

    /** @return true to replace, false to append */
    public boolean isReplace() {
        return get(REPLACE);
    }

    /** @return suffix for appended column names */
    public String getSuffix() {
        return getOrDefault(SUFFIX, "_binned");
    }

    /** @return custom bin names */
    public String[] getBinNames() {
        return getOrDefault(BIN_NAMES, new String[0]);
    }

    /** @return custom bin left bounds */
    public String[] getBinLeftBounds() {
        return getOrDefault(BIN_LEFT_BOUNDS, new String[0]);
    }

    /** @return custom bin left inclusive flags */
    public String[] getBinLeftInclusive() {
        return getOrDefault(BIN_LEFT_INCLUSIVE, new String[0]);
    }

    /** @return custom bin right bounds */
    public String[] getBinRightBounds() {
        return getOrDefault(BIN_RIGHT_BOUNDS, new String[0]);
    }

    /** @return custom bin right inclusive flags */
    public String[] getBinRightInclusive() {
        return getOrDefault(BIN_RIGHT_INCLUSIVE, new String[0]);
    }
}
