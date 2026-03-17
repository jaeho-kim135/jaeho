package org.knime.bigdata.spark3_5.dx.jobs.preproc.cellsplit;

import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.cellsplit.SparkCellSplitterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.cellsplit.SparkCellSplitterJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.try_element_at;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.trim;

/**
 * Spark job that splits a string column into multiple columns using a delimiter.
 * Uses Spark's {@code split()} function to tokenize and {@code element_at()} to extract parts.
 */
@SparkClass
public class CellSplitterJob implements SparkJob<SparkCellSplitterJobInput, SparkCellSplitterJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkCellSplitterJobOutput runJob(final SparkContext sparkContext, final SparkCellSplitterJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final String namedOutputObject = input.getFirstNamedOutputObject();
        final Dataset<Row> inputDF = namedObjects.getDataFrame(namedInputObject);

        final String srcCol = input.getColumn();
        // For literal delimiters, escape regex special chars; for regex, use as-is
        final String delimiter = input.isUseRegex() ? input.getDelimiter() : Pattern.quote(input.getDelimiter());

        // 1. Determine number of output columns
        final int numCols;
        if (input.isAutoMode()) {
            final int scanLimit = input.getScanLimit();
            final Dataset<Row> scanDF = inputDF.limit(scanLimit);
            final Row maxRow = scanDF
                .select(size(split(col("`" + srcCol + "`"), delimiter)).as("sz"))
                .agg(max("sz"))
                .first();

            if (maxRow == null || maxRow.isNullAt(0)) {
                numCols = 1;
            } else {
                // agg(max(size(...))) may return IntegerType or LongType depending on Spark version
                numCols = ((Number) maxRow.get(0)).intValue();
            }
        } else {
            numCols = input.getFixedSize();
        }

        if (numCols < 1) {
            throw new KNIMESparkException("The determined number of output columns is less than 1. "
                + "The input data may be empty or all values in column '" + srcCol + "' are null.");
        }

        // 2. Apply split and extract elements
        // Materialize the split array once as a temporary column to avoid recomputing split() per output column
        final String tmpSplitCol = "__knime_split_tmp_" + UUID.randomUUID().toString().replace("-", "") + "__";
        final String prefix = (input.getOutputPrefix() == null || input.getOutputPrefix().isEmpty())
            ? srcCol : input.getOutputPrefix();

        Dataset<Row> result = inputDF.withColumn(tmpSplitCol,
            split(col("`" + srcCol + "`"), delimiter));

        for (int i = 0; i < numCols; i++) {
            Column elem = try_element_at(col(tmpSplitCol), lit(i + 1)); // 1-based, ANSI-safe
            if (input.isTrim()) {
                elem = trim(elem);
            }
            if (input.isUseEmptyString()) {
                elem = coalesce(elem, lit(""));
            }
            result = result.withColumn(prefix + "_" + (i + 1), elem);
        }

        // Remove the temporary split column
        result = result.drop(tmpSplitCol);

        // 3. Remove input column if requested
        if (input.isRemoveInputCol()) {
            result = result.drop(col("`" + srcCol + "`"));
        }

        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkCellSplitterJobOutput(namedOutputObject, outputSchema);
    }
}
