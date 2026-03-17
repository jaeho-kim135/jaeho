package org.knime.bigdata.spark3_4.dx.jobs.preproc.rowsplitter;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.rowsplitter.SparkRowSplitterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.rowsplitter.SparkRowSplitterJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.not;

import org.apache.spark.sql.Column;

/**
 * Spark job that splits a DataFrame into matching and non-matching rows
 * based on filter conditions.
 */
@SparkClass
public class RowSplitterJob implements SparkJob<SparkRowSplitterJobInput, SparkRowSplitterJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkRowSplitterJobOutput runJob(final SparkContext sparkContext, final SparkRowSplitterJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final Dataset<Row> inputDF = namedObjects.getDataFrame(input.getFirstNamedInputObject());

        final String condition = buildCondition(input, inputDF);

        // Split into matches and non-matches.
        // Use not().or(isNull()) to ensure rows where the condition evaluates to NULL
        // (e.g., due to NULL column values) go to the non-matching output instead of being lost.
        final Column filterCol = expr(condition);
        final Dataset<Row> matches = inputDF.filter(filterCol);
        final Dataset<Row> nonMatches = inputDF.filter(not(filterCol).or(filterCol.isNull()));

        // Register output DataFrames
        final String matchOutputId = input.getNamedOutputObjects().get(0);
        final String nonMatchOutputId = input.getNamedOutputObjects().get(1);

        namedObjects.addDataFrame(matchOutputId, matches);
        namedObjects.addDataFrame(nonMatchOutputId, nonMatches);

        // Build output specs
        final IntermediateSpec matchSpec = TypeConverters.convertSpec(matches.schema());
        final IntermediateSpec nonMatchSpec = TypeConverters.convertSpec(nonMatches.schema());

        return new SparkRowSplitterJobOutput(matchOutputId, matchSpec, nonMatchOutputId, nonMatchSpec);
    }

    /**
     * Builds the SQL filter condition string from the job input.
     */
    private String buildCondition(final SparkRowSplitterJobInput input, final Dataset<Row> inputDF) {
        final String[] columns = input.getColumns();
        final String[] operators = input.getOperators();
        final String[] values = input.getValues();
        final String[] upperValues = input.getUpperValues();
        final boolean[] caseSensitives = input.getCaseSensitives();
        final String logicOp = input.getMatchCriteria();

        final List<String> conditions = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            final String c = "`" + columns[i].replace("`", "``") + "`";
            final boolean isNumeric = isNumericColumn(inputDF.schema(), columns[i]);
            final boolean cs = caseSensitives[i];
            final String v = isNumeric ? values[i] : "'" + escapeSQL(values[i]) + "'";
            final String cExpr = (!cs && !isNumeric) ? "lower(" + c + ")" : c;
            final String vExpr = (!cs && !isNumeric) ? "lower(" + v + ")" : v;

            switch (operators[i]) {
                case "EQ":
                    conditions.add(cExpr + " = " + vExpr);
                    break;
                case "NEQ":
                    conditions.add(cExpr + " != " + vExpr);
                    break;
                case "GT":
                    conditions.add(cExpr + " > " + vExpr);
                    break;
                case "GTE":
                    conditions.add(cExpr + " >= " + vExpr);
                    break;
                case "LT":
                    conditions.add(cExpr + " < " + vExpr);
                    break;
                case "LTE":
                    conditions.add(cExpr + " <= " + vExpr);
                    break;
                case "BETWEEN": {
                    final String upper = isNumeric
                        ? upperValues[i]
                        : "'" + escapeSQL(upperValues[i]) + "'";
                    final String upperExpr = (!cs && !isNumeric) ? "lower(" + upper + ")" : upper;
                    conditions.add(cExpr + " BETWEEN " + vExpr + " AND " + upperExpr);
                    break;
                }
                case "LIKE":
                    conditions.add(cExpr + " LIKE " + vExpr);
                    break;
                case "REGEX": {
                    final String regexPat = cs
                        ? escapeSQL(values[i])
                        : "(?i)" + escapeSQL(values[i]);
                    conditions.add(c + " RLIKE '" + regexPat + "'");
                    break;
                }
                case "IS_NULL":
                    conditions.add(c + " IS NULL");
                    break;
                case "IS_NOT_NULL":
                    conditions.add(c + " IS NOT NULL");
                    break;
                case "IS_TRUE":
                    conditions.add(c + " = true");
                    break;
                case "IS_FALSE":
                    conditions.add(c + " = false");
                    break;
                default:
                    conditions.add(cExpr + " = " + vExpr);
                    break;
            }
        }

        return "(" + String.join(" " + logicOp + " ", conditions) + ")";
    }

    /**
     * Determines whether a column is of a numeric type.
     */
    private boolean isNumericColumn(final StructType schema, final String colName) {
        try {
            final StructField field = schema.apply(colName);
            final DataType dt = field.dataType();
            return dt instanceof IntegerType
                || dt instanceof LongType
                || dt instanceof DoubleType
                || dt instanceof FloatType
                || dt instanceof ShortType
                || dt instanceof ByteType
                || dt instanceof DecimalType;
        } catch (final Exception e) {
            return false;
        }
    }

    /**
     * Escapes single quotes in a SQL string value.
     */
    private String escapeSQL(final String str) {
        if (str == null) {
            return "";
        }
        return str.replace("'", "''");
    }
}
