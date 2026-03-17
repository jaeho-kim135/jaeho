package org.knime.bigdata.spark.dx.node.preproc.rowsplitter;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Row Splitter job.
 * Contains parallel arrays describing filter predicates and the match criteria.
 */
@SparkClass
public class SparkRowSplitterJobInput extends JobInput {

    private static final String MATCH_CRITERIA = "matchCriteria";
    private static final String COLUMNS = "columns";
    private static final String OPERATORS = "operators";
    private static final String VALUES = "values";
    private static final String UPPER_VALUES = "upperValues";
    private static final String CASE_SENSITIVES = "caseSensitives";

    /** Deserialization constructor. */
    public SparkRowSplitterJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param matchOutputObject named output object ID for matching rows
     * @param nonMatchOutputObject named output object ID for non-matching rows
     * @param matchCriteria "AND" or "OR"
     * @param columns column names for each predicate
     * @param operators operator names for each predicate
     * @param values comparison values for each predicate
     * @param upperValues upper bound values for BETWEEN predicates
     * @param caseSensitives case sensitivity flags for each predicate
     */
    public SparkRowSplitterJobInput(final String inputObject,
            final String matchOutputObject, final String nonMatchOutputObject,
            final String matchCriteria,
            final String[] columns, final String[] operators,
            final String[] values, final String[] upperValues,
            final boolean[] caseSensitives) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(matchOutputObject);
        addNamedOutputObject(nonMatchOutputObject);
        set(MATCH_CRITERIA, matchCriteria);
        set(COLUMNS, columns);
        set(OPERATORS, operators);
        set(VALUES, values);
        set(UPPER_VALUES, upperValues);
        // Convert boolean[] to String[] for serialization
        final String[] csStrings = new String[caseSensitives.length];
        for (int i = 0; i < caseSensitives.length; i++) {
            csStrings[i] = String.valueOf(caseSensitives[i]);
        }
        set(CASE_SENSITIVES, csStrings);
    }

    /** @return the match criteria ("AND" or "OR") */
    public String getMatchCriteria() {
        return get(MATCH_CRITERIA);
    }

    /** @return the column names */
    public String[] getColumns() {
        return get(COLUMNS);
    }

    /** @return the operator names */
    public String[] getOperators() {
        return get(OPERATORS);
    }

    /** @return the comparison values */
    public String[] getValues() {
        return get(VALUES);
    }

    /** @return the upper bound values for BETWEEN */
    public String[] getUpperValues() {
        return get(UPPER_VALUES);
    }

    /**
     * @return the case sensitivity flags
     */
    public boolean[] getCaseSensitives() {
        final String[] csStrings = get(CASE_SENSITIVES);
        final boolean[] result = new boolean[csStrings.length];
        for (int i = 0; i < csStrings.length; i++) {
            result[i] = Boolean.parseBoolean(csStrings[i]);
        }
        return result;
    }
}
