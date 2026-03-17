package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringreplacer;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.stringreplacer.SparkStringReplacerJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringreplacer.SparkStringReplacerJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

/**
 * Spark job that replaces strings in a column using literal, wildcard, or regex matching.
 * Uses Spark SQL functions: {@code replace()}, {@code regexp_replace()}, and {@code CASE WHEN}.
 */
@SparkClass
public class StringReplacerJob implements SparkJob<SparkStringReplacerJobInput, SparkStringReplacerJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkStringReplacerJobOutput runJob(final SparkContext sparkContext,
            final SparkStringReplacerJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String col = "`" + input.getColumn() + "`";
        final String pattern = input.getPattern();
        final String replacement = input.getReplacement();
        final String outputCol = input.getOutputColName();
        final String patternType = input.getPatternType();
        final String strategy = input.getReplacementStrategy();
        final boolean caseSensitive = input.isCaseSensitive();

        final String sparkExpr = buildExpression(col, patternType, pattern, replacement,
            caseSensitive, input.isEnableEscaping(), strategy);

        final Dataset<Row> result;
        try {
            result = inputFrame.withColumn(outputCol, expr(sparkExpr));
        } catch (final Exception e) {
            throw new KNIMESparkException("String replacement failed: " + e.getMessage(), e);
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkStringReplacerJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * Builds the Spark SQL expression for the string replacement.
     */
    private String buildExpression(final String col, final String patternType,
            final String pattern, final String replacement,
            final boolean caseSensitive, final boolean enableEscaping,
            final String strategy) {

        switch (patternType) {
            case "LITERAL":
                return buildLiteralExpression(col, pattern, replacement, caseSensitive, strategy);
            case "WILDCARD":
                return buildWildcardExpression(col, pattern, replacement,
                    caseSensitive, enableEscaping, strategy);
            case "REGEX":
                return buildRegexExpression(col, pattern, replacement, caseSensitive, strategy);
            default:
                // Default to literal
                return buildLiteralExpression(col, pattern, replacement, caseSensitive, strategy);
        }
    }

    /**
     * Builds expression for LITERAL pattern matching.
     */
    private String buildLiteralExpression(final String col, final String pattern,
            final String replacement, final boolean caseSensitive, final String strategy) {

        if ("ALL_OCCURRENCES".equals(strategy)) {
            if (caseSensitive) {
                // Case-sensitive literal replace: use Spark's replace() function
                return "replace(" + col + ", '" + escapeSQL(pattern) + "', '"
                    + escapeSQL(replacement) + "')";
            } else {
                // Case-insensitive: use regexp_replace with (?i) flag and escaped regex chars
                return "regexp_replace(" + col + ", '(?i)" + escapeSQL(escapeRegex(pattern)) + "', '"
                    + escapeLiteralReplacement(replacement) + "')";
            }
        } else {
            // WHOLE_STRING: replace entire cell only if whole string matches
            final String cond;
            if (caseSensitive) {
                cond = col + " = '" + escapeSQL(pattern) + "'";
            } else {
                cond = "lower(" + col + ") = lower('" + escapeSQL(pattern) + "')";
            }
            return "CASE WHEN " + cond + " THEN '" + escapeSQL(replacement) + "' ELSE " + col + " END";
        }
    }

    /**
     * Builds expression for WILDCARD pattern matching.
     * Converts wildcard pattern (* and ?) to regex, then uses regexp_replace().
     */
    private String buildWildcardExpression(final String col, final String pattern,
            final String replacement, final boolean caseSensitive,
            final boolean enableEscaping, final String strategy) {

        final String regex;
        if (enableEscaping) {
            regex = wildcardToRegexWithEscaping(pattern);
        } else {
            regex = wildcardToRegex(pattern);
        }

        final String regexPattern = caseSensitive ? regex : "(?i)" + regex;

        if ("ALL_OCCURRENCES".equals(strategy)) {
            return "regexp_replace(" + col + ", '" + escapeSQL(regexPattern) + "', '"
                + escapeLiteralReplacement(replacement) + "')";
        } else {
            // WHOLE_STRING: match anchored
            return "CASE WHEN " + col + " RLIKE '^" + escapeSQL(regexPattern) + "$' THEN '"
                + escapeSQL(replacement) + "' ELSE " + col + " END";
        }
    }

    /**
     * Builds expression for REGEX pattern matching.
     */
    private String buildRegexExpression(final String col, final String pattern,
            final String replacement, final boolean caseSensitive, final String strategy) {

        // In Regex mode, backreferences ($1, $2, etc.) are supported by Spark's regexp_replace.
        // The replacement string is passed through as-is (not SQL-escaped for backreferences).

        if ("ALL_OCCURRENCES".equals(strategy)) {
            final String regexPattern = caseSensitive ? pattern : "(?i)" + pattern;
            return "regexp_replace(" + col + ", '" + escapeSQL(regexPattern) + "', '"
                + escapeReplacement(replacement) + "')";
        } else {
            // WHOLE_STRING: full match with anchors
            final String anchoredPattern = caseSensitive
                ? "^" + pattern + "$"
                : "(?i)^" + pattern + "$";
            return "CASE WHEN " + col + " RLIKE '" + escapeSQL(anchoredPattern)
                + "' THEN regexp_replace(" + col + ", '" + escapeSQL(anchoredPattern)
                + "', '" + escapeReplacement(replacement) + "') ELSE " + col + " END";
        }
    }

    /**
     * Escapes special regex characters in a literal string so it can be used as a literal
     * pattern inside a regex.
     *
     * @param literal the literal string to escape
     * @return the regex-escaped string
     */
    private static String escapeRegex(final String literal) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < literal.length(); i++) {
            final char c = literal.charAt(i);
            if (".[]{}()*+?^$|\\".indexOf(c) >= 0) {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Escapes single quotes for SQL string literals using ANSI SQL standard ('' doubling).
     *
     * @param value the string to escape
     * @return the SQL-escaped string
     */
    private static String escapeSQL(final String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }

    /**
     * Escapes replacement text for use in Spark SQL regexp_replace in REGEX mode.
     * Preserves backreferences ($1, $2, etc.) while escaping single quotes.
     *
     * @param replacement the replacement text
     * @return the escaped replacement text
     */
    private static String escapeReplacement(final String replacement) {
        if (replacement == null) {
            return "";
        }
        return replacement.replace("'", "''");
    }

    /**
     * Escapes replacement text for non-REGEX modes (LITERAL, WILDCARD) that use regexp_replace().
     * Escapes both single quotes (for SQL) and $ (to prevent backreference interpretation).
     *
     * @param replacement the replacement text
     * @return the escaped replacement text safe for literal replacement in regexp_replace
     */
    private static String escapeLiteralReplacement(final String replacement) {
        if (replacement == null) {
            return "";
        }
        return replacement.replace("\\", "\\\\").replace("$", "\\$").replace("'", "''");
    }

    /**
     * Converts a wildcard pattern to a regex pattern.
     * {@code *} becomes {@code .*} and {@code ?} becomes {@code .}
     * All other regex special characters are escaped.
     *
     * @param wildcard the wildcard pattern
     * @return the equivalent regex pattern
     */
    private static String wildcardToRegex(final String wildcard) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < wildcard.length(); i++) {
            final char c = wildcard.charAt(i);
            switch (c) {
                case '*':
                    sb.append(".*");
                    break;
                case '?':
                    sb.append('.');
                    break;
                default:
                    if (".[]{}()+^$|\\".indexOf(c) >= 0) {
                        sb.append('\\');
                    }
                    sb.append(c);
                    break;
            }
        }
        return sb.toString();
    }

    /**
     * Converts a wildcard pattern to a regex pattern with escaping support.
     * Supports {@code \?} for literal question mark and {@code \\} for literal backslash.
     * {@code *} becomes {@code .*} and unescaped {@code ?} becomes {@code .}
     *
     * @param wildcard the wildcard pattern with potential escape sequences
     * @return the equivalent regex pattern
     */
    private static String wildcardToRegexWithEscaping(final String wildcard) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < wildcard.length(); i++) {
            final char c = wildcard.charAt(i);
            if (c == '\\' && i + 1 < wildcard.length()) {
                final char next = wildcard.charAt(i + 1);
                if (next == '?') {
                    // \? → literal question mark
                    sb.append("\\?");
                    i++;
                } else if (next == '\\') {
                    // \\ → literal backslash
                    sb.append("\\\\");
                    i++;
                } else if (next == '*') {
                    // \* → literal asterisk
                    sb.append("\\*");
                    i++;
                } else {
                    // Unknown escape: treat backslash as literal
                    sb.append("\\\\");
                }
            } else {
                switch (c) {
                    case '*':
                        sb.append(".*");
                        break;
                    case '?':
                        sb.append('.');
                        break;
                    default:
                        if (".[]{}()+^$|\\".indexOf(c) >= 0) {
                            sb.append('\\');
                        }
                        sb.append(c);
                        break;
                }
            }
        }
        return sb.toString();
    }
}
