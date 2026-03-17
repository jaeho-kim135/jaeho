package org.knime.bigdata.spark3_5.dx.jobs.preproc.duplicates;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;

/**
 * Spark job that filters or annotates duplicate rows using Window Functions.
 * Supports REMOVE mode (FIRST/LAST/MINIMUM/MAXIMUM/REMOVE_ALL) and
 * KEEP mode (annotate with status column).
 */
@SparkClass
public class DuplicateRowFilterJob
    implements SparkJob<SparkDuplicateRowFilterJobInput, SparkDuplicateRowFilterJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkDuplicateRowFilterJobOutput runJob(final SparkContext sparkContext,
            final SparkDuplicateRowFilterJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputDF = namedObjects.getDataFrame(namedInputObject);

        final String[] dupCols = input.getDuplicateColumns();
        final String duplicateHandling = input.getDuplicateHandling();
        final String rowSelection = input.getRowSelection();
        final String orderCol = input.getOrderColumn();
        final String orderDirection = input.getOrderDirection();
        final boolean addStatusColumn = input.isAddStatusColumn();
        final String statusColumnName = input.getStatusColumnName();

        // If no duplicate columns specified, use all columns
        final Column[] partitionCols;
        if (dupCols == null || dupCols.length == 0) {
            final String[] allCols = inputDF.columns();
            partitionCols = toColumns(allCols);
        } else {
            partitionCols = toColumns(dupCols);
        }

        Dataset<Row> result;

        if ("KEEP".equals(duplicateHandling)) {
            // KEEP mode: retain all rows, optionally annotate with status column
            result = handleKeepMode(inputDF, partitionCols, orderCol, addStatusColumn, statusColumnName);

        } else if ("REMOVE_ALL".equals(rowSelection)) {
            // REMOVE + REMOVE_ALL: keep only unique rows (count == 1)
            result = handleRemoveAll(inputDF, partitionCols);

        } else if ("MINIMUM".equals(rowSelection) || "MAXIMUM".equals(rowSelection)) {
            // REMOVE + MINIMUM/MAXIMUM: keep row with min/max value in order column
            result = handleMinMax(inputDF, partitionCols, orderCol, "MINIMUM".equals(rowSelection));

        } else {
            // REMOVE + FIRST/LAST: keep first or last occurrence based on order column
            result = handleFirstLast(inputDF, partitionCols, orderCol, orderDirection,
                "LAST".equals(rowSelection));
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkDuplicateRowFilterJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * KEEP mode: retain all rows. If addStatusColumn is true, add a status column
     * with values "unique", "chosen", or "duplicate".
     */
    private Dataset<Row> handleKeepMode(final Dataset<Row> inputDF, final Column[] partitionCols,
            final String orderCol, final boolean addStatusColumn, final String statusColumnName) {

        if (addStatusColumn) {
            final String safeOrderCol = orderCol.replace("`", "``");
            final WindowSpec countWindow = Window.partitionBy(partitionCols);
            Dataset<Row> withCount = inputDF.withColumn("__dup_count__", count("*").over(countWindow));

            final WindowSpec rowWindow = Window.partitionBy(partitionCols)
                .orderBy(col("`" + safeOrderCol + "`").asc_nulls_last());
            Dataset<Row> withRn = withCount.withColumn("__rn__", row_number().over(rowWindow));

            final Column statusCol = when(col("__dup_count__").equalTo(1), lit("unique"))
                .when(col("__rn__").equalTo(1), lit("chosen"))
                .otherwise(lit("duplicate"));

            return withRn.withColumn(statusColumnName, statusCol)
                .drop("__dup_count__", "__rn__");
        } else {
            // No status column needed in KEEP mode — return input as-is
            return inputDF;
        }
    }

    /**
     * REMOVE + REMOVE_ALL: keep only rows that have no duplicates (count == 1).
     */
    private Dataset<Row> handleRemoveAll(final Dataset<Row> inputDF, final Column[] partitionCols) {
        final WindowSpec countWindow = Window.partitionBy(partitionCols);
        return inputDF.withColumn("__cnt__", count("*").over(countWindow))
            .filter(col("__cnt__").equalTo(1))
            .drop("__cnt__");
    }

    /**
     * REMOVE + MINIMUM/MAXIMUM: keep the row with the minimum or maximum value
     * in the order column within each duplicate group.
     */
    private Dataset<Row> handleMinMax(final Dataset<Row> inputDF, final Column[] partitionCols,
            final String orderCol, final boolean useMin) {

        final String safeOrderCol = orderCol.replace("`", "``");
        final Column orderExpr = useMin
            ? col("`" + safeOrderCol + "`").asc_nulls_last()
            : col("`" + safeOrderCol + "`").desc_nulls_last();

        final WindowSpec window = Window.partitionBy(partitionCols).orderBy(orderExpr);
        return inputDF.withColumn("__rn__", row_number().over(window))
            .filter(col("__rn__").equalTo(1))
            .drop("__rn__");
    }

    /**
     * REMOVE + FIRST/LAST: keep the first or last occurrence based on
     * the order column and direction.
     */
    private Dataset<Row> handleFirstLast(final Dataset<Row> inputDF, final Column[] partitionCols,
            final String orderCol, final String orderDirection, final boolean isLast) {

        final String safeOrderCol = orderCol.replace("`", "``");
        boolean ascending = "ASC".equals(orderDirection);
        if (isLast) {
            ascending = !ascending; // LAST reverses the sort direction
        }

        final Column orderExpr = ascending
            ? col("`" + safeOrderCol + "`").asc_nulls_last()
            : col("`" + safeOrderCol + "`").desc_nulls_last();

        final WindowSpec window = Window.partitionBy(partitionCols).orderBy(orderExpr);
        return inputDF.withColumn("__rn__", row_number().over(window))
            .filter(col("__rn__").equalTo(1))
            .drop("__rn__");
    }

    /**
     * Converts an array of column names to an array of Spark Column objects.
     * Column names are backtick-quoted to handle special characters.
     */
    private Column[] toColumns(final String[] cols) {
        final Column[] result = new Column[cols.length];
        for (int i = 0; i < cols.length; i++) {
            result[i] = col("`" + cols[i].replace("`", "``") + "`");
        }
        return result;
    }
}
