package org.knime.bigdata.spark3_4.dx.jobs.preproc.binner;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.binner.SparkNumericBinnerJobInput;
import org.knime.bigdata.spark.dx.node.preproc.binner.SparkNumericBinnerJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.max;

/**
 * Spark job that bins numeric columns into categorical labels using CASE WHEN expressions.
 * Supports equal-width, equal-frequency, and custom range binning modes.
 */
@SparkClass
public class NumericBinnerJob implements SparkJob<SparkNumericBinnerJobInput, SparkNumericBinnerJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkNumericBinnerJobOutput runJob(final SparkContext sparkContext,
            final SparkNumericBinnerJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();
        final String binningMode = input.getBinningMode();
        final boolean replace = input.isReplace();
        final String suffix = input.getSuffix();

        if (columns == null || columns.length == 0) {
            throw new KNIMESparkException("No columns specified for binning.");
        }

        Dataset<Row> result = inputFrame;

        for (final String colName : columns) {
            final String[][] bins; // [i][0]=name, [1]=leftBound, [2]=leftInclusive, [3]=rightBound, [4]=rightInclusive

            if ("EQUAL_WIDTH".equals(binningMode) || "EQUAL_FREQUENCY".equals(binningMode)) {
                bins = computeAutoBins(inputFrame, colName, binningMode,
                    input.getNumberOfBins(), input.getBinNaming());

                if (bins == null) {
                    // All nulls in column - output null
                    final String outputCol = replace ? colName : colName + suffix;
                    result = result.withColumn(outputCol, lit(null).cast(DataTypes.StringType));
                    continue;
                }
            } else {
                // CUSTOM mode
                bins = reconstructCustomBins(input);
            }

            // Build CASE WHEN expression
            final String caseExpr = buildCaseWhenExpression(colName, bins);

            final String outputCol = replace ? colName : colName + suffix;
            result = result.withColumn(outputCol, expr(caseExpr));
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkNumericBinnerJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * Computes automatic bin definitions based on data statistics.
     *
     * @return array of bin definitions, or null if the column contains only nulls
     */
    private String[][] computeAutoBins(final Dataset<Row> df, final String colName,
            final String binningMode, final int numberOfBins, final String binNaming) {

        // Calculate min/max
        final Row minMax = df.agg(
            min(col("`" + colName + "`").cast(DataTypes.DoubleType)),
            max(col("`" + colName + "`").cast(DataTypes.DoubleType))
        ).first();

        if (minMax.isNullAt(0) || minMax.isNullAt(1)) {
            return null; // All nulls
        }

        final double minVal = minMax.getDouble(0);
        final double maxVal = minMax.getDouble(1);
        int n = numberOfBins;

        double[] edges;
        if ("EQUAL_WIDTH".equals(binningMode)) {
            if (minVal == maxVal) {
                // Single value: create one bin containing that value
                edges = new double[]{minVal, maxVal};
                n = 1;
            } else {
                final double width = (maxVal - minVal) / n;
                edges = new double[n + 1];
                for (int i = 0; i <= n; i++) {
                    edges[i] = (i == n) ? maxVal : minVal + i * width;
                }
            }
        } else {
            // EQUAL_FREQUENCY - use approximate quantiles
            final double[] quantiles = new double[n + 1];
            for (int i = 0; i <= n; i++) {
                quantiles[i] = (double) i / n;
            }
            edges = df.stat().approxQuantile("`" + colName + "`", quantiles, 0.01);

            // Remove duplicate edges (quantiles can produce duplicates for skewed data)
            final List<Double> uniqueEdges = new ArrayList<>();
            for (final double e : edges) {
                if (uniqueEdges.isEmpty() || e != uniqueEdges.get(uniqueEdges.size() - 1)) {
                    uniqueEdges.add(e);
                }
            }
            edges = new double[uniqueEdges.size()];
            for (int i = 0; i < uniqueEdges.size(); i++) {
                edges[i] = uniqueEdges.get(i);
            }
            n = edges.length - 1;

            if (n <= 0) {
                // All same value
                edges = new double[]{minVal, maxVal};
                n = 1;
            }
        }

        // Build bin definitions
        final String[][] bins = new String[n][5];
        for (int i = 0; i < n; i++) {
            final double left = edges[i];
            final double right = edges[i + 1];
            final boolean lastBin = (i == n - 1);

            final String name;
            switch (binNaming != null ? binNaming : "BORDERS") {
                case "NUMBERED":
                    name = "Bin " + (i + 1);
                    break;
                case "MIDPOINTS":
                    name = String.valueOf((left + right) / 2);
                    break;
                case "BORDERS":
                default:
                    name = "[" + left + ", " + right + (lastBin ? "]" : ")");
                    break;
            }

            bins[i][0] = name;
            bins[i][1] = String.valueOf(left);
            bins[i][2] = "true";  // left inclusive
            bins[i][3] = String.valueOf(right);
            bins[i][4] = String.valueOf(lastBin);  // right inclusive only for last bin
        }

        return bins;
    }

    /**
     * Reconstructs custom bin definitions from the job input's parallel arrays.
     */
    private String[][] reconstructCustomBins(final SparkNumericBinnerJobInput input) throws KNIMESparkException {
        final String[] names = input.getBinNames();
        final String[] lefts = input.getBinLeftBounds();
        final String[] leftIncl = input.getBinLeftInclusive();
        final String[] rights = input.getBinRightBounds();
        final String[] rightIncl = input.getBinRightInclusive();

        if (names == null || names.length == 0) {
            throw new KNIMESparkException("No custom bin definitions provided.");
        }

        final String[][] bins = new String[names.length][5];
        for (int i = 0; i < names.length; i++) {
            bins[i][0] = names[i];
            bins[i][1] = i < lefts.length ? lefts[i] : "0.0";
            bins[i][2] = i < leftIncl.length ? leftIncl[i] : "true";
            bins[i][3] = i < rights.length ? rights[i] : "0.0";
            bins[i][4] = i < rightIncl.length ? rightIncl[i] : "false";
        }
        return bins;
    }

    /**
     * Builds a SQL CASE WHEN expression for binning a single column.
     *
     * @param colName the column name (will be backtick-quoted)
     * @param bins array of bin definitions [name, leftBound, leftInclusive, rightBound, rightInclusive]
     * @return the CASE WHEN SQL expression string
     */
    private String buildCaseWhenExpression(final String colName, final String[][] bins) {
        final StringBuilder sb = new StringBuilder("CASE");

        for (final String[] bin : bins) {
            final String binName = bin[0];
            final double leftBound = Double.parseDouble(bin[1]);
            final boolean leftInclusive = Boolean.parseBoolean(bin[2]);
            final double rightBound = Double.parseDouble(bin[3]);
            final boolean rightInclusive = Boolean.parseBoolean(bin[4]);

            final String leftOp = leftInclusive ? ">=" : ">";
            final String rightOp = rightInclusive ? "<=" : "<";

            final String quotedCol = "`" + colName + "`";

            final String cond;
            if (leftBound == Double.NEGATIVE_INFINITY && rightBound == Double.POSITIVE_INFINITY) {
                // Unbounded on both sides: always matches
                cond = quotedCol + " IS NOT NULL";
            } else if (leftBound == Double.NEGATIVE_INFINITY) {
                cond = quotedCol + " " + rightOp + " " + formatDouble(rightBound);
            } else if (rightBound == Double.POSITIVE_INFINITY) {
                cond = quotedCol + " " + leftOp + " " + formatDouble(leftBound);
            } else {
                cond = quotedCol + " " + leftOp + " " + formatDouble(leftBound)
                    + " AND " + quotedCol + " " + rightOp + " " + formatDouble(rightBound);
            }

            // Escape single quotes in bin name (SQL standard: '' for literal ')
            final String escapedName = binName.replace("'", "''");
            sb.append(" WHEN ").append(cond).append(" THEN '").append(escapedName).append("'");
        }

        sb.append(" ELSE NULL END");
        return sb.toString();
    }

    /**
     * Formats a double value for use in SQL, handling special cases.
     */
    private String formatDouble(final double val) {
        if (val == (long) val) {
            return String.valueOf((long) val) + ".0";
        }
        return String.valueOf(val);
    }
}
