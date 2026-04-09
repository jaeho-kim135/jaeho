package org.knime.bigdata.spark3_5.dx.jobs.preproc.rank;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankJobInput;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

/**
 * Spark job that computes rank values.
 * Uses driver-side processing (collectAsList + createDataFrame) to avoid
 * shuffle operations that hang on Livy/JDK8 environments.
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

        final StructType schema = inputFrame.schema();
        final List<Row> allRows = inputFrame.collectAsList();

        // Resolve ranking column indices and sort directions
        final int[] rankColIndices = new int[rankingColumns.length];
        final boolean[] ascending = new boolean[rankingColumns.length];
        for (int i = 0; i < rankingColumns.length; i++) {
            rankColIndices[i] = schema.fieldIndex(rankingColumns[i]);
            ascending[i] = i < rankingOrders.length ? "ASCENDING".equals(rankingOrders[i]) : true;
        }

        // Resolve group column indices
        final int[] groupColIndices;
        if (groupColumns != null && groupColumns.length > 0) {
            groupColIndices = new int[groupColumns.length];
            for (int i = 0; i < groupColumns.length; i++) {
                groupColIndices[i] = schema.fieldIndex(groupColumns[i]);
            }
        } else {
            groupColIndices = new int[0];
        }

        // Group rows by group columns (preserving original indices)
        final Map<String, List<Integer>> groups = new LinkedHashMap<>();
        for (int i = 0; i < allRows.size(); i++) {
            final String key = groupKey(allRows.get(i), groupColIndices);
            List<Integer> indices = groups.get(key);
            if (indices == null) {
                indices = new ArrayList<>();
                groups.put(key, indices);
            }
            indices.add(i);
        }

        // Compute ranks per group
        final boolean useLong = !"INTEGER".equals(rankDataType);
        final Number[] ranks = new Number[allRows.size()];

        for (final List<Integer> groupIndices : groups.values()) {
            // Sort indices within group by ranking columns
            final List<Integer> sorted = new ArrayList<>(groupIndices);
            Collections.sort(sorted, new Comparator<Integer>() {
                @Override
                public int compare(final Integer a, final Integer b) {
                    return compareRows(allRows.get(a), allRows.get(b),
                        rankColIndices, ascending, missingToEnd);
                }
            });

            // Assign ranks within sorted group
            assignRanks(sorted, allRows, rankColIndices, rankMode, ranks, useLong);
        }

        // Build result rows preserving original order
        final StructType resultSchema = schema.add(outputColName,
            useLong ? DataTypes.LongType : DataTypes.IntegerType);

        final List<Row> resultRows = new ArrayList<>(allRows.size());
        for (int i = 0; i < allRows.size(); i++) {
            final Row row = allRows.get(i);
            final Object[] values = new Object[row.size() + 1];
            for (int j = 0; j < row.size(); j++) {
                values[j] = row.get(j);
            }
            values[row.size()] = ranks[i];
            resultRows.add(RowFactory.create(values));
        }

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final Dataset<Row> result = spark.createDataFrame(resultRows, resultSchema);

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkRankJobOutput(namedOutputObject, outputSchema);
    }

    private void assignRanks(final List<Integer> sorted, final List<Row> allRows,
            final int[] rankColIndices, final String rankMode,
            final Number[] ranks, final boolean useLong) {

        for (int pos = 0; pos < sorted.size(); pos++) {
            final int rowIdx = sorted.get(pos);
            final int rankValue;

            if ("ORDINAL".equals(rankMode)) {
                // ROW_NUMBER: always position + 1
                rankValue = pos + 1;
            } else if (pos == 0) {
                rankValue = 1;
            } else {
                final int prevRowIdx = sorted.get(pos - 1);
                final boolean tie = isTie(allRows.get(rowIdx), allRows.get(prevRowIdx), rankColIndices);

                if (tie) {
                    rankValue = ranks[prevRowIdx].intValue();
                } else if ("DENSE".equals(rankMode)) {
                    // DENSE_RANK: increment by 1 (no gaps)
                    rankValue = ranks[prevRowIdx].intValue() + 1;
                } else {
                    // STANDARD RANK: rank = position + 1 (gaps after ties)
                    rankValue = pos + 1;
                }
            }

            ranks[rowIdx] = useLong ? Long.valueOf(rankValue) : Integer.valueOf(rankValue);
        }
    }

    private boolean isTie(final Row a, final Row b, final int[] rankColIndices) {
        for (final int idx : rankColIndices) {
            final Object va = a.get(idx);
            final Object vb = b.get(idx);
            if (va == null && vb == null) {
                continue;
            }
            if (va == null || vb == null) {
                return false;
            }
            if (!va.equals(vb)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareRows(final Row a, final Row b, final int[] rankColIndices,
            final boolean[] ascending, final boolean missingToEnd) {

        for (int i = 0; i < rankColIndices.length; i++) {
            final Object va = a.get(rankColIndices[i]);
            final Object vb = b.get(rankColIndices[i]);

            if (va == null && vb == null) {
                continue;
            }
            if (va == null) {
                // missingToEnd: nulls always go to end
                // default: asc → nulls first (-1), desc → nulls last (1)
                return missingToEnd ? 1 : (ascending[i] ? -1 : 1);
            }
            if (vb == null) {
                return missingToEnd ? -1 : (ascending[i] ? 1 : -1);
            }

            final int cmp = (va instanceof Comparable)
                ? ((Comparable) va).compareTo(vb)
                : va.toString().compareTo(vb.toString());

            if (cmp != 0) {
                return ascending[i] ? cmp : -cmp;
            }
        }
        return 0;
    }

    private String groupKey(final Row row, final int[] indices) {
        if (indices.length == 0) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        for (final int idx : indices) {
            if (sb.length() > 0) {
                sb.append('\0');
            }
            final Object val = row.get(idx);
            sb.append(val == null ? "\1NULL\1" : val.toString());
        }
        return sb.toString();
    }
}
