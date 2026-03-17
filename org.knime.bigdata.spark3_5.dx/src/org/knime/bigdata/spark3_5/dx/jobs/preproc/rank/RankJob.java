package org.knime.bigdata.spark3_5.dx.jobs.preproc.rank;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankJobInput;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.dense_rank;
import static org.apache.spark.sql.functions.row_number;

/**
 * Spark job that computes rank values using SQL window functions.
 * Supports RANK(), DENSE_RANK(), and ROW_NUMBER() with optional partitioning.
 */
@SparkClass
public class RankJob implements SparkJob<SparkRankJobInput, SparkRankJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkRankJobOutput runJob(final SparkContext sparkContext, final SparkRankJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] rankingColumns = input.getRankingColumns();
        final String[] rankingOrders = input.getRankingOrders();
        final String[] groupColumns = input.getGroupColumns();
        final String rankMode = input.getRankMode();
        final String outputColName = input.getOutputColName();
        final String rankDataType = input.getRankDataType();
        final boolean missingToEnd = input.isMissingToEnd();

        if (rankingColumns == null || rankingColumns.length == 0) {
            throw new KNIMESparkException(
                "No ranking columns specified. Please add at least one ranking criterion.");
        }

        // Build order columns with null handling
        final Column[] orderCols = new Column[rankingColumns.length];
        for (int i = 0; i < rankingColumns.length; i++) {
            final Column c = col("`" + rankingColumns[i] + "`");
            final boolean ascending = i < rankingOrders.length
                ? "ASCENDING".equals(rankingOrders[i])
                : true;

            if (ascending) {
                orderCols[i] = missingToEnd ? c.asc_nulls_last() : c.asc();
            } else {
                orderCols[i] = missingToEnd ? c.desc_nulls_last() : c.desc();
            }
        }

        // Build WindowSpec
        WindowSpec windowSpec;
        if (groupColumns != null && groupColumns.length > 0) {
            final Column[] partitionCols = new Column[groupColumns.length];
            for (int i = 0; i < groupColumns.length; i++) {
                partitionCols[i] = col("`" + groupColumns[i] + "`");
            }
            windowSpec = Window.partitionBy(partitionCols).orderBy(orderCols);
        } else {
            windowSpec = Window.orderBy(orderCols);
        }

        // Apply rank function
        Column rankCol;
        switch (rankMode) {
            case "DENSE":
                rankCol = dense_rank().over(windowSpec);
                break;
            case "ORDINAL":
                rankCol = row_number().over(windowSpec);
                break;
            case "STANDARD":
            default:
                rankCol = rank().over(windowSpec);
                break;
        }

        // Cast to appropriate data type
        // Spark window functions (rank, dense_rank, row_number) natively return IntegerType.
        // Only cast when LONG is selected.
        if (!"INTEGER".equals(rankDataType)) {
            rankCol = rankCol.cast(DataTypes.LongType);
        }

        // Add rank column to DataFrame
        final Dataset<Row> result = inputFrame.withColumn(outputColName, rankCol);

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkRankJobOutput(namedOutputObject, outputSchema);
    }
}
