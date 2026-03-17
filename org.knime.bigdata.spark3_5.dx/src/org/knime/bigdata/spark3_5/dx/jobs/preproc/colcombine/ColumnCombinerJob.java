package org.knime.bigdata.spark3_5.dx.jobs.preproc.colcombine;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.colcombine.SparkColumnCombinerJobInput;
import org.knime.bigdata.spark.dx.node.preproc.colcombine.SparkColumnCombinerJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.when;

/**
 * Spark job that combines multiple columns into a single string column
 * using CONCAT_WS. Supports missing value handling, quoting, and
 * delimiter replacement.
 */
@SparkClass
public class ColumnCombinerJob implements SparkJob<SparkColumnCombinerJobInput, SparkColumnCombinerJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkColumnCombinerJobOutput runJob(final SparkContext sparkContext,
            final SparkColumnCombinerJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();
        final String delimiter = input.getDelimiter();
        final String outputColName = input.getOutputColName();
        final boolean removeInputCols = input.isRemoveInputCols();
        final String handleMissing = input.getHandleMissing();
        final String quoteMode = input.getQuoteMode();
        final String quoteChar = input.getQuoteChar();
        final String replacementDelimiter = input.getReplacementDelimiter();

        if (columns.length < 2) {
            throw new KNIMESparkException("At least 2 columns must be selected for combining.");
        }

        // 1. Prepare column expressions (cast to String + handle missing)
        Column[] cols;
        if ("AS_EMPTY".equals(handleMissing)) {
            cols = Arrays.stream(columns)
                .map(c -> coalesce(col("`" + c + "`").cast(DataTypes.StringType), lit("")))
                .toArray(Column[]::new);
        } else {
            // SKIP: CONCAT_WS naturally skips null values
            cols = Arrays.stream(columns)
                .map(c -> col("`" + c + "`").cast(DataTypes.StringType))
                .toArray(Column[]::new);
        }

        // 2. QuoteMode processing
        if ("QUOTE".equals(quoteMode)) {
            final String qc = quoteChar;
            final String delim = delimiter;
            // Escape existing quote chars inside cells by doubling them (CSV-style)
            final String quotedQc = Pattern.quote(qc);
            final String doubledQc = (qc + qc).replace("\\", "\\\\").replace("$", "\\$");
            cols = Arrays.stream(cols)
                .map(c -> when(c.contains(delim),
                    concat(lit(qc), regexp_replace(c, quotedQc, doubledQc), lit(qc))).otherwise(c))
                .toArray(Column[]::new);
        } else if ("REPLACE_IN_CELL".equals(quoteMode)) {
            final String quotedDelimiter = Pattern.quote(delimiter);
            // Escape \ and $ in replacement to prevent regex backreference interpretation
            final String replDelim = replacementDelimiter.replace("\\", "\\\\").replace("$", "\\$");
            cols = Arrays.stream(cols)
                .map(c -> regexp_replace(c, quotedDelimiter, replDelim))
                .toArray(Column[]::new);
        }

        // 3. Combine using concat_ws
        Dataset<Row> result = inputFrame.withColumn(outputColName, concat_ws(delimiter, cols));

        // 4. Remove input columns if requested
        if (removeInputCols) {
            for (String c : columns) {
                if (!c.equals(outputColName)) {
                    result = result.drop(col("`" + c + "`"));
                }
            }
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkColumnCombinerJobOutput(namedOutputObject, outputSchema);
    }
}
