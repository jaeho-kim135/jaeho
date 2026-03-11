package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringtonumber;

import java.util.ArrayList;
import java.util.List;
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
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

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

        final org.apache.spark.sql.types.DataType sparkType = switch (parseType) {
            case "INTEGER" -> DataTypes.IntegerType;
            case "LONG" -> DataTypes.LongType;
            default -> DataTypes.DoubleType;
        };

        Dataset<Row> result = inputFrame;

        // Process each column
        for (final String colName : columns) {
            // Build the transformation chain as a single column expression

            // Step 1: Blank -> null
            Column expr = when(trim(col(colName)).equalTo(lit("")), lit(null).cast(DataTypes.StringType))
                .otherwise(col(colName));

            // Step 2: Thousands separator removal (if configured)
            if (!thousandsSep.isEmpty()) {
                expr = regexp_replace(expr, Pattern.quote(thousandsSep), "");
            }

            // Step 3: Decimal separator handling (for non-"." separators when target needs decimal)
            final boolean needsDecimalHandling = !decimalSep.isEmpty() && !".".equals(decimalSep)
                && ("DOUBLE".equals(parseType));
            if (needsDecimalHandling) {
                // 3a: If value contains ".", it's ambiguous -> null
                final Column containsDot = expr.contains(".");
                // 3b: Replace custom decimal separator with "."
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

            if (failOnError) {
                // Save original string value and add validity marker before converting
                final String origCol = "_stn_orig_" + colName;
                final String validCol = "_stn_valid_" + colName;
                result = result.withColumn(origCol, col(colName));
                result = result.withColumn(validCol,
                    trim(col(colName)).isNotNull().and(trim(col(colName)).notEqual(lit(""))));

                // Replace the original column with the converted value
                result = result.withColumn(colName, expr);

                // Step 7: Check for conversion failures
                final long failCount = result
                    .filter(col(validCol).equalTo(lit(true)).and(col(colName).isNull()))
                    .count();

                if (failCount > 0) {
                    // Collect sample failing values from the saved original column
                    final List<Row> failingSamples = result
                        .filter(col(validCol).equalTo(lit(true)).and(col(colName).isNull()))
                        .select(col(origCol))
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

                // Remove temp columns
                result = result.drop(origCol).drop(validCol);
            } else {
                // Simply replace the column
                result = result.withColumn(colName, expr);
            }
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkStringToNumberJobOutput(namedOutputObject, outputSchema);
    }
}
