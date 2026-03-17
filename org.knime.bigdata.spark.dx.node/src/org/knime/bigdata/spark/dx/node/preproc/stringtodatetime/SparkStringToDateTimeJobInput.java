package org.knime.bigdata.spark.dx.node.preproc.stringtodatetime;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark String to Date&Time job.
 */
@SparkClass
public class SparkStringToDateTimeJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String FORMAT = "format";
    private static final String OUTPUT_TYPE = "output_type";
    private static final String LOCALE = "locale";
    private static final String FAIL_ON_ERROR = "fail_on_error";

    /** Deserialization constructor. */
    public SparkStringToDateTimeJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject the named input object ID
     * @param outputObject the named output object ID
     * @param columns the columns to convert
     * @param format the date/time format pattern
     * @param outputType the output type (DATE, TIME, DATE_TIME, ZONED_DATE_TIME)
     * @param locale the locale language tag
     * @param failOnError whether to fail on parsing errors
     */
    public SparkStringToDateTimeJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String format, final String outputType,
            final String locale, final boolean failOnError) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(FORMAT, format);
        set(OUTPUT_TYPE, outputType);
        set(LOCALE, locale);
        set(FAIL_ON_ERROR, failOnError);
    }

    /** @return the columns to convert */
    public String[] getColumns() { return get(COLUMNS); }

    /** @return the format pattern */
    public String getFormat() { return get(FORMAT); }

    /** @return the output type (DATE, TIME, DATE_TIME, ZONED_DATE_TIME) */
    public String getOutputType() { return get(OUTPUT_TYPE); }

    /** @return the locale language tag */
    public String getLocale() { return get(LOCALE); }

    /** @return whether to fail on parsing errors */
    public boolean isFailOnError() { return get(FAIL_ON_ERROR); }
}
