package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringtonumber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.stringtonumber.SparkStringToNumberJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringtonumber.SparkStringToNumberJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;

/**
 * Spark job that converts String columns to numeric types using DataFrame API.
 */
@SparkClass
public class StringToNumberJob implements SparkJob<SparkStringToNumberJobInput, SparkStringToNumberJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkStringToNumberJobOutput runJob(final SparkContext sparkContext,
            final SparkStringToNumberJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();
        final String parseType = input.getParseType();
        final String decimalSep = input.getDecimalSeparator();
        final String thousandsSep = input.getThousandsSeparator();
        final boolean genericParse = input.isGenericParse();
        final boolean failOnError = input.isFailOnError();

        final org.apache.spark.sql.types.DataType sparkType;
        if ("INTEGER".equals(parseType)) {
            sparkType = DataTypes.IntegerType;
        } else if ("LONG".equals(parseType)) {
            sparkType = DataTypes.LongType;
        } else {
            sparkType = DataTypes.DoubleType;
        }

        final Set<String> targetSet = new HashSet<>(Arrays.asList(columns));
        final String[] allColumns = inputFrame.columns();

        // Build all column expressions in a single select() to avoid
        // repeated withColumn() calls which can cause column resolution issues
        final List<Column> selectCols = new ArrayList<>();
        for (final String c : allColumns) {
            if (targetSet.contains(c)) {
                selectCols.add(buildConversionExpr(c, thousandsSep, decimalSep,
                    parseType, genericParse, sparkType).alias(c));
            } else {
                selectCols.add(col("`" + c + "`"));
            }
        }

        Dataset<Row> result;

        if (failOnError) {
            // First pass: create DataFrame with both original and converted columns
            final List<Column> validationCols = new ArrayList<>();
            for (final String c : allColumns) {
                validationCols.add(col("`" + c + "`"));
                if (targetSet.contains(c)) {
                    validationCols.add(buildConversionExpr(c, thousandsSep, decimalSep,
                        parseType, genericParse, sparkType).alias("_stn_conv_" + c));
                    validationCols.add(
                        trim(col("`" + c + "`")).isNotNull()
                            .and(trim(col("`" + c + "`")).notEqual(lit("")))
                            .alias("_stn_valid_" + c));
                }
            }
            final Dataset<Row> validFrame =
                inputFrame.select(validationCols.toArray(new Column[0]));

            // Check each target column for conversion failures
            for (final String colName : columns) {
                final long failCount = validFrame
                    .filter(col("_stn_valid_" + colName).equalTo(lit(true))
                        .and(col("_stn_conv_" + colName).isNull()))
                    .count();

                if (failCount > 0) {
                    final List<Row> failingSamples = validFrame
                        .filter(col("_stn_valid_" + colName).equalTo(lit(true))
                            .and(col("_stn_conv_" + colName).isNull()))
                        .select(col("`" + colName + "`"))
                        .limit(3)
                        .collectAsList();

                    final List<String> sampleValues = new ArrayList<>();
                    for (final Row r : failingSamples) {
                        sampleValues.add(r.isNullAt(0) ? "null" : "\"" + r.getString(0) + "\"");
                    }

                    throw new KNIMESparkException("Failed to convert " + failCount
                        + " rows in column '" + colName + "'. Sample failing values: "
                        + String.join(", ", sampleValues));
                }
            }

            // Final projection: pick converted columns for targets, originals for rest
            final List<Column> finalCols = new ArrayList<>();
            for (final String c : allColumns) {
                if (targetSet.contains(c)) {
                    finalCols.add(col("_stn_conv_" + c).alias(c));
                } else {
                    finalCols.add(col("`" + c + "`"));
                }
            }
            result = validFrame.select(finalCols.toArray(new Column[0]));
        } else {
            // Simple case: single select with all transformations
            result = inputFrame.select(selectCols.toArray(new Column[0]));
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkStringToNumberJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * Builds the conversion expression for a single column.
     */
    private static Column buildConversionExpr(final String colName, final String thousandsSep,
            final String decimalSep, final String parseType, final boolean genericParse,
            final org.apache.spark.sql.types.DataType sparkType) {

        // Step 1: Blank -> null
        Column expr = when(trim(col("`" + colName + "`")).equalTo(lit("")),
                lit(null).cast(DataTypes.StringType))
            .otherwise(col("`" + colName + "`"));

        // Step 2: Thousands separator removal (if configured)
        if (!thousandsSep.isEmpty()) {
            expr = regexp_replace(expr, Pattern.quote(thousandsSep), "");
        }

        // Step 3: Decimal separator handling (for non-"." separators when target needs decimal)
        final boolean needsDecimalHandling = !decimalSep.isEmpty() && !".".equals(decimalSep)
            && ("DOUBLE".equals(parseType));
        if (needsDecimalHandling) {
            final Column containsDot = expr.contains(".");
            final Column replaced = regexp_replace(expr, Pattern.quote(decimalSep), ".");
            expr = when(containsDot, lit(null).cast(DataTypes.StringType))
                .otherwise(replaced);
        }

        // Step 4: Suffix check (when genericParse=false, reject d/D/f/F suffixes)
        if (!genericParse) {
            final Column hasSuffix = expr.rlike("(?i).*[df]$");
            expr = when(hasSuffix, lit(null).cast(DataTypes.StringType))
                .otherwise(expr);
        }

        // Step 5: Trim whitespace
        expr = trim(expr);

        // Step 6: Cast to target type (Spark returns null for invalid values)
        expr = expr.cast(sparkType);

        return expr;
    }
}
