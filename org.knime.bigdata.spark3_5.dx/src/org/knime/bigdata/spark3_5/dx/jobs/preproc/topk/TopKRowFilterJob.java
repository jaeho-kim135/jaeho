package org.knime.bigdata.spark3_5.dx.jobs.preproc.topk;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.topk.SparkTopKRowFilterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.topk.SparkTopKRowFilterJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.dense_rank;

/**
 * Spark job that performs the Top k Row Filter operation.
 * Supports ROWS and UNIQUE_VALUES filter modes, per-group top-k via window functions,
 * configurable sort order, null handling, and output ordering.
 */
@SparkClass
public class TopKRowFilterJob implements SparkJob<SparkTopKRowFilterJobInput, SparkTopKRowFilterJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkTopKRowFilterJobOutput runJob(final SparkContext sparkContext, final SparkTopKRowFilterJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final Dataset<Row> inputFrame = namedObjects.getDataFrame(input.getFirstNamedInputObject());

        final long k = input.getK();
        final String filterMode = input.getFilterMode();
        final String outputOrder = input.getOutputOrder();
        final boolean missingsToEnd = input.isMissingsToEnd();
        final String[] groupCols = input.getGroupColumns();

        // Build order columns with null handling
        final Column[] orderCols = buildOrderColumns(input, missingsToEnd);

        Dataset<Row> result;

        if (groupCols.length == 0) {
            // === No grouping ===
            if ("ROWS".equals(filterMode)) {
                result = inputFrame.orderBy(orderCols).limit((int) k);
            } else {
                // UNIQUE_VALUES: top k distinct value combinations, then all matching rows
                final List<String> sortColNames = getSortColumnNames(input);
                final Column[] sortNameCols = sortColNames.stream()
                    .map(c -> col("`" + c + "`")).toArray(Column[]::new);
                final Dataset<Row> topK = inputFrame.select(sortNameCols)
                    .distinct().orderBy(orderCols).limit((int) k);
                result = inputFrame.join(topK,
                    scala.collection.JavaConverters.asScalaBuffer(sortColNames).toList(), "leftsemi");
            }
        } else {
            // === With grouping - use window functions ===
            final Column[] partCols = Arrays.stream(groupCols)
                .map(c -> col("`" + c + "`")).toArray(Column[]::new);
            final WindowSpec window = Window.partitionBy(partCols).orderBy(orderCols);

            if ("ROWS".equals(filterMode)) {
                result = inputFrame.withColumn("__rn__", row_number().over(window))
                    .filter(col("__rn__").leq(k))
                    .drop("__rn__");
            } else {
                // UNIQUE_VALUES with dense_rank
                result = inputFrame.withColumn("__dr__", dense_rank().over(window))
                    .filter(col("__dr__").leq(k))
                    .drop("__dr__");
            }
        }

        // Apply output order
        if ("SORTED".equals(outputOrder)) {
            result = result.orderBy(orderCols);
        }
        // ARBITRARY: no additional sort

        // Validation-only mode: return preview without writing output
        if (input.isValidateOnly()) {
            final SparkTopKRowFilterJobOutput output = new SparkTopKRowFilterJobOutput(null, null);
            try {
                output.setPreviewData(result.showString(5, 20, false));
            } catch (final Exception e) {
                output.setPreviewData("Preview failed: " + e.getMessage());
            }
            return output;
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkTopKRowFilterJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * Builds the order columns array from the job input, applying null handling.
     */
    private Column[] buildOrderColumns(final SparkTopKRowFilterJobInput input, final boolean missingsToEnd) {
        final String[] sortCols = input.getSortColumns();
        final String[] sortOrders = input.getSortOrders();
        final Column[] cols = new Column[sortCols.length];
        for (int i = 0; i < sortCols.length; i++) {
            final String order = (i < sortOrders.length) ? sortOrders[i] : "DESCENDING";
            cols[i] = buildOrderColumn(sortCols[i], order, missingsToEnd);
        }
        return cols;
    }

    /**
     * Builds a single order column with sort direction and null handling.
     */
    private Column buildOrderColumn(final String colName, final String order, final boolean missingsToEnd) {
        final Column c = col("`" + colName + "`");
        if ("DESCENDING".equals(order)) {
            return missingsToEnd ? c.desc_nulls_last() : c.desc();
        } else {
            return missingsToEnd ? c.asc_nulls_last() : c.asc();
        }
    }

    /**
     * Gets the list of sort column names from the job input.
     */
    private List<String> getSortColumnNames(final SparkTopKRowFilterJobInput input) {
        return Arrays.asList(input.getSortColumns());
    }
}
