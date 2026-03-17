package org.knime.bigdata.spark.dx.node.preproc.caseconvert;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Case Converter job.
 * Contains the selected column names, case mode, and validate-only flag.
 */
@SparkClass
public class SparkCaseConvertJobInput extends JobInput {

    private static final String COLUMNS = "columns";
    private static final String MODE = "caseMode";
    private static final String VALIDATE_ONLY = "validateOnly";

    /** Deserialization constructor. */
    public SparkCaseConvertJobInput() {
    }

    /**
     * Normal execution constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param columns the string column names to convert
     * @param mode the case mode (UPPERCASE, LOWERCASE, TITLE_CASE)
     */
    public SparkCaseConvertJobInput(final String inputObject, final String outputObject,
            final String[] columns, final String mode) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(COLUMNS, columns);
        set(MODE, mode);
        set(VALIDATE_ONLY, false);
    }

    /**
     * Validation-only constructor (for Evaluate button).
     *
     * @param inputObject named input object ID
     * @param columns the string column names to convert
     * @param mode the case mode (UPPERCASE, LOWERCASE, TITLE_CASE)
     */
    public SparkCaseConvertJobInput(final String inputObject,
            final String[] columns, final String mode) {
        addNamedInputObject(inputObject);
        set(COLUMNS, columns);
        set(MODE, mode);
        set(VALIDATE_ONLY, true);
    }

    /** @return the column names to convert */
    public String[] getColumns() {
        return get(COLUMNS);
    }

    /** @return the case mode (UPPERCASE, LOWERCASE, TITLE_CASE) */
    public String getMode() {
        return get(MODE);
    }

    /** @return true if this is a validation-only run (no output produced) */
    public boolean isValidateOnly() {
        return get(VALIDATE_ONLY);
    }
}
