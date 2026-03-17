package org.knime.bigdata.spark.dx.node.preproc.stringtonumber;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark String to Number job.
 */
@SparkClass
public class SparkStringToNumberJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String PARSE_TYPE = "parseType";
    private static final String DECIMAL_SEPARATOR = "decimalSeparator";
    private static final String THOUSANDS_SEPARATOR = "thousandsSeparator";
    private static final String GENERIC_PARSE = "genericParse";
    private static final String FAIL_ON_ERROR = "failOnError";

    /** Deserialization constructor. */
    public SparkStringToNumberJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param columns the columns to convert
     * @param parseType the target type (INTEGER, DOUBLE, LONG)
     * @param decimalSeparator the decimal separator character
     * @param thousandsSeparator the thousands separator character
     * @param genericParse whether to allow d/D/f/F suffixes
     * @param failOnError whether to fail on conversion error
     */
    public SparkStringToNumberJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String parseType,
            final String decimalSeparator, final String thousandsSeparator,
            final boolean genericParse, final boolean failOnError) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(PARSE_TYPE, parseType);
        set(DECIMAL_SEPARATOR, decimalSeparator);
        set(THOUSANDS_SEPARATOR, thousandsSeparator);
        set(GENERIC_PARSE, genericParse);
        set(FAIL_ON_ERROR, failOnError);
    }

    /** @return the columns to convert */
    public String[] getColumns() { return get(COLUMNS); }

    /** @return the target type (INTEGER, DOUBLE, LONG) */
    public String getParseType() { return get(PARSE_TYPE); }

    /** @return the decimal separator character */
    public String getDecimalSeparator() { return getOrDefault(DECIMAL_SEPARATOR, "."); }

    /** @return the thousands separator character */
    public String getThousandsSeparator() { return getOrDefault(THOUSANDS_SEPARATOR, ""); }

    /** @return whether generic parsing is enabled */
    public boolean isGenericParse() { return get(GENERIC_PARSE); }

    /** @return whether to fail on conversion error */
    public boolean isFailOnError() { return get(FAIL_ON_ERROR); }
}
