package org.knime.bigdata.spark3_4.dx.jobs.sql.ruleengine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.sql.ruleengine.SparkRuleEngineJobInput;
import org.knime.bigdata.spark.dx.node.sql.ruleengine.SparkRuleEngineJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.expr;

/**
 * Spark job that converts IF-THEN rules to CASE WHEN expressions.
 * Parses rule lines, converts KNIME rule syntax to Spark SQL,
 * and applies the resulting CASE WHEN expression using withColumn().
 */
@SparkClass
public class RuleEngineJob implements SparkJob<SparkRuleEngineJobInput, SparkRuleEngineJobOutput> {

    private static final long serialVersionUID = 1L;

    /** Pattern for $col$ column references. */
    private static final Pattern COL_REF_PATTERN = Pattern.compile("\\$([^$]+)\\$");

    @Override
    public SparkRuleEngineJobOutput runJob(final SparkContext sparkContext, final SparkRuleEngineJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] rules = input.getRules();
        final String defaultValue = input.getDefaultValue();
        final boolean defaultIsMissing = input.isDefaultMissing();
        final boolean isReplace = input.isReplace();
        final String outputColumn = input.getOutputColumn();
        final boolean validateOnly = input.isValidateOnly();

        // Build the default expression
        final String defaultExpr = buildDefaultExpression(defaultValue, defaultIsMissing);

        // Build the CASE WHEN expression from rules
        final String caseWhenExpr;
        try {
            caseWhenExpr = buildCaseWhen(rules, defaultExpr);
        } catch (final Exception e) {
            throw new KNIMESparkException("Error building CASE WHEN expression: " + e.getMessage(), e);
        }

        // Apply the expression
        final Dataset<Row> result;
        try {
            result = inputFrame.withColumn(outputColumn, expr(caseWhenExpr));
        } catch (final Exception e) {
            throw new KNIMESparkException(
                "Error applying rule expression: " + e.getMessage()
                + "\nGenerated SQL: " + caseWhenExpr, e);
        }

        if (validateOnly) {
            final String preview = result.showString(10, 40, false);
            final SparkRuleEngineJobOutput output = new SparkRuleEngineJobOutput(null, null);
            output.setPreviewData(preview);
            return output;
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkRuleEngineJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * Builds the default expression for the ELSE clause.
     *
     * @param defaultValue the user-specified default value
     * @param defaultIsMissing true if default should be NULL
     * @return a Spark SQL expression string for the ELSE clause
     */
    private String buildDefaultExpression(final String defaultValue, final boolean defaultIsMissing) {
        if (defaultIsMissing) {
            return "NULL";
        }
        if (defaultValue == null || defaultValue.isEmpty()) {
            return "NULL";
        }
        // Check if it's a $col$ column reference
        if (defaultValue.matches("^\\$([^$]+)\\$$")) {
            final String colName = defaultValue.substring(1, defaultValue.length() - 1);
            return "`" + colName.replace("`", "``") + "`";
        }
        // Check if it's a number
        try {
            Double.parseDouble(defaultValue);
            return defaultValue;
        } catch (final NumberFormatException e) {
            // Not a number, treat as string literal
        }
        // Check for TRUE/FALSE
        if ("TRUE".equalsIgnoreCase(defaultValue) || "FALSE".equalsIgnoreCase(defaultValue)) {
            return defaultValue.toUpperCase();
        }
        // String literal - escape single quotes
        return "'" + escapeSQL(defaultValue) + "'";
    }

    /**
     * Builds a CASE WHEN expression from rule lines.
     *
     * @param rules array of rule lines
     * @param defaultExpr the ELSE expression
     * @return complete CASE WHEN ... END expression
     */
    private String buildCaseWhen(final String[] rules, final String defaultExpr) {
        final StringBuilder sb = new StringBuilder("CASE");
        boolean hasRule = false;

        for (final String rule : rules) {
            final String trimmed = rule.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("//")) {
                continue; // skip blank lines and comments
            }

            final int idx = findArrowSeparator(trimmed);
            if (idx < 0) {
                continue; // skip lines without =>
            }

            final String condition = convertCondition(trimmed.substring(0, idx).trim());
            final String outcome = convertOutcome(trimmed.substring(idx + 2).trim());

            sb.append(" WHEN ").append(condition).append(" THEN ").append(outcome);
            hasRule = true;
        }

        if (!hasRule) {
            // No valid rules found, just return the default
            return defaultExpr;
        }

        sb.append(" ELSE ").append(defaultExpr).append(" END");
        return sb.toString();
    }

    /**
     * Converts a KNIME rule condition to Spark SQL syntax.
     * <ul>
     *   <li>{@code $col$} becomes {@code `col`}</li>
     *   <li>{@code "text"} becomes {@code 'text'}</li>
     *   <li>{@code IS MISSING} becomes {@code IS NULL}</li>
     *   <li>{@code IS NOT MISSING} becomes {@code IS NOT NULL}</li>
     *   <li>{@code MATCHES "regex"} becomes {@code RLIKE 'regex'}</li>
     *   <li>{@code A XOR B} becomes {@code ((A) AND NOT (B)) OR (NOT (A) AND (B))}</li>
     * </ul>
     */
    private String convertCondition(final String condition) {
        String result = condition;

        // $col$ -> `col` (with backtick escaping)
        result = replaceColumnRefs(result);

        // "text" -> 'text' (with proper escaping of single quotes inside)
        result = convertQuotes(result);

        // IS NOT MISSING -> IS NOT NULL (must be before IS MISSING)
        result = result.replaceAll("(?i)IS\\s+NOT\\s+MISSING", "IS NOT NULL");

        // IS MISSING -> IS NULL
        result = result.replaceAll("(?i)IS\\s+MISSING", "IS NULL");

        // MATCHES 'regex' -> RLIKE 'regex'
        result = result.replaceAll("(?i)MATCHES\\s+", "RLIKE ");

        // Handle XOR: A XOR B -> ((A) AND NOT (B)) OR (NOT (A) AND (B))
        // Iteratively replace the first XOR found (not inside quoted strings) until none remain.
        result = replaceXor(result);

        return result;
    }

    /**
     * Converts a KNIME rule outcome to Spark SQL syntax.
     * <ul>
     *   <li>{@code $col$} becomes {@code `col`}</li>
     *   <li>{@code "text"} becomes {@code 'text'}</li>
     *   <li>{@code TRUE}/{@code FALSE} stay as-is</li>
     *   <li>Numbers stay as-is</li>
     * </ul>
     */
    private String convertOutcome(final String outcome) {
        String result = outcome;

        // $col$ -> `col` (with backtick escaping)
        result = replaceColumnRefs(result);

        // "text" -> 'text' (with proper escaping of single quotes inside)
        result = convertQuotes(result);

        // TRUE/FALSE and numbers stay as-is
        return result;
    }

    /**
     * Converts double-quoted strings to single-quoted SQL strings,
     * escaping any single quotes inside the quoted text.
     * E.g., {@code "it's good"} becomes {@code 'it''s good'}.
     */
    private String convertQuotes(final String input) {
        final StringBuilder sb = new StringBuilder();
        boolean inQuote = false;
        final StringBuilder quoted = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);
            if (c == '"') {
                if (inQuote) {
                    // End of quoted string - emit as SQL single-quoted string
                    sb.append("'").append(quoted.toString().replace("'", "''")).append("'");
                    quoted.setLength(0);
                }
                inQuote = !inQuote;
            } else if (inQuote) {
                quoted.append(c);
            } else {
                sb.append(c);
            }
        }
        // If we ended still in a quote (malformed), just append what we have
        if (inQuote) {
            sb.append("'").append(quoted.toString().replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    /**
     * Iteratively replaces XOR operators with their boolean equivalent.
     * {@code A XOR B} becomes {@code ((A) AND NOT (B)) OR (NOT (A) AND (B))}.
     * Handles multiple XOR operators by replacing the first one found
     * (outside of single-quoted strings) in each iteration.
     *
     * @param input the expression that may contain XOR operators
     * @return the expression with all XOR operators expanded
     */
    private String replaceXor(final String input) {
        String result = input;
        int xorPos;
        while ((xorPos = findXorOutsideQuotes(result)) >= 0) {
            final String left = result.substring(0, xorPos).trim();
            final String right = result.substring(xorPos + 3).trim(); // 3 = length of "XOR"
            result = "((" + left + ") AND NOT (" + right + ")) OR (NOT (" + left + ") AND (" + right + "))";
        }
        return result;
    }

    /**
     * Finds the position of the first {@code XOR} keyword (case-insensitive)
     * that is not inside a single-quoted SQL string.
     * Requires whitespace (or start/end of string) on both sides of the keyword.
     *
     * @param input the expression to search
     * @return the index of the 'X' in the first unquoted XOR, or -1 if none found
     */
    private int findXorOutsideQuotes(final String input) {
        boolean inQuote = false;
        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);
            if (c == '\'') {
                // Handle escaped single quotes ('') inside strings
                if (inQuote && i + 1 < input.length() && input.charAt(i + 1) == '\'') {
                    i++; // skip escaped quote
                } else {
                    inQuote = !inQuote;
                }
            } else if (!inQuote && i + 3 <= input.length()) {
                final String sub = input.substring(i, Math.min(i + 3, input.length()));
                if (sub.equalsIgnoreCase("XOR")) {
                    // Check that XOR is surrounded by whitespace (or at string boundaries)
                    final boolean leftOk = (i == 0) || Character.isWhitespace(input.charAt(i - 1));
                    final boolean rightOk = (i + 3 >= input.length())
                            || Character.isWhitespace(input.charAt(i + 3));
                    if (leftOk && rightOk) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Replaces {@code $col$} column references with backtick-quoted identifiers,
     * escaping any embedded backtick characters in the column name.
     *
     * @param input the expression containing {@code $col$} references
     * @return the expression with column references replaced by {@code `col`}
     */
    private String replaceColumnRefs(final String input) {
        final Matcher m = COL_REF_PATTERN.matcher(input);
        final StringBuffer sb = new StringBuffer();
        while (m.find()) {
            final String colName = m.group(1).replace("`", "``");
            m.appendReplacement(sb, Matcher.quoteReplacement("`" + colName + "`"));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * Finds the position of the {@code =>} rule separator that is not inside
     * a quoted string (single or double quotes). Single-quoted strings may
     * appear from flow-variable resolution; double-quoted strings are the
     * original KNIME rule syntax.
     *
     * @param line the rule line to search
     * @return the index of {@code =} in {@code =>}, or -1 if not found
     */
    private int findArrowSeparator(final String line) {
        boolean inDoubleQuote = false;
        boolean inSingleQuote = false;
        for (int i = 0; i < line.length() - 1; i++) {
            final char c = line.charAt(i);
            if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (c == '\'' && !inDoubleQuote) {
                if (inSingleQuote && i + 1 < line.length() && line.charAt(i + 1) == '\'') {
                    i++; // skip escaped single quote ''
                } else {
                    inSingleQuote = !inSingleQuote;
                }
            } else if (!inDoubleQuote && !inSingleQuote
                    && c == '=' && line.charAt(i + 1) == '>') {
                return i;
            }
        }
        return -1;
    }

    /**
     * Escapes single quotes in a SQL string literal.
     */
    private static String escapeSQL(final String value) {
        return value.replace("'", "''");
    }
}
