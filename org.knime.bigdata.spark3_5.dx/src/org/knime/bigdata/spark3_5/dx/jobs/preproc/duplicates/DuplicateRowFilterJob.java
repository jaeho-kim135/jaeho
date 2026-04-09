package org.knime.bigdata.spark3_5.dx.jobs.preproc.duplicates;

import java.util.ArrayList;
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
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterJobInput;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

/**
 * Spark job that filters or annotates duplicate rows.
 * Uses driver-side processing (collectAsList + createDataFrame) to avoid
 * shuffle operations that hang on Livy/JDK8 environments.
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

        final String[] effectiveDupCols;
        if (dupCols == null || dupCols.length == 0) {
            effectiveDupCols = inputDF.columns();
        } else {
            effectiveDupCols = dupCols;
        }

        final StructType schema = inputDF.schema();
        final List<Row> allRows = inputDF.collectAsList();

        final int[] dupColIndices = new int[effectiveDupCols.length];
        for (int i = 0; i < effectiveDupCols.length; i++) {
            dupColIndices[i] = schema.fieldIndex(effectiveDupCols[i]);
        }
        final int orderColIdx = (orderCol != null && !orderCol.isEmpty())
            ? schema.fieldIndex(orderCol) : -1;

        List<Row> resultRows;
        StructType resultSchema = schema;

        if ("KEEP".equals(duplicateHandling)) {
            if (addStatusColumn) {
                final boolean keepMin = calculateKeepMin(rowSelection, orderDirection);
                resultRows = processKeepWithStatus(allRows, dupColIndices, orderColIdx, keepMin);
                resultSchema = schema.add(statusColumnName, DataTypes.StringType);
            } else {
                resultRows = allRows;
            }
        } else if ("REMOVE_ALL".equals(rowSelection)) {
            resultRows = processRemoveAll(allRows, dupColIndices);
        } else {
            final boolean keepMin = calculateKeepMin(rowSelection, orderDirection);
            resultRows = processRemoveKeepOne(allRows, dupColIndices, orderColIdx, keepMin);
        }

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final Dataset<Row> result = spark.createDataFrame(resultRows, resultSchema);

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkDuplicateRowFilterJobOutput(namedOutputObject, outputSchema);
    }

    /** REMOVE + FIRST/LAST/MINIMUM/MAXIMUM: keep one row per group. */
    private List<Row> processRemoveKeepOne(final List<Row> allRows, final int[] dupColIndices,
            final int orderColIdx, final boolean keepMin) {
        final Map<String, Row> best = new LinkedHashMap<>();
        for (final Row row : allRows) {
            final String key = groupKey(row, dupColIndices);
            final Row existing = best.get(key);
            if (existing == null) {
                best.put(key, row);
            } else if (orderColIdx >= 0
                    && shouldReplace(row.get(orderColIdx), existing.get(orderColIdx), keepMin)) {
                best.put(key, row);
            }
        }
        return new ArrayList<>(best.values());
    }

    /** REMOVE + REMOVE_ALL: keep only rows that have no duplicates. */
    private List<Row> processRemoveAll(final List<Row> allRows, final int[] dupColIndices) {
        final Map<String, List<Row>> groups = new LinkedHashMap<>();
        for (final Row row : allRows) {
            final String key = groupKey(row, dupColIndices);
            List<Row> group = groups.get(key);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(key, group);
            }
            group.add(row);
        }
        final List<Row> result = new ArrayList<>();
        for (final List<Row> group : groups.values()) {
            if (group.size() == 1) {
                result.add(group.get(0));
            }
        }
        return result;
    }

    /** KEEP + status column: annotate each row as unique/chosen/duplicate. */
    private List<Row> processKeepWithStatus(final List<Row> allRows, final int[] dupColIndices,
            final int orderColIdx, final boolean keepMin) {
        final Map<String, List<Integer>> groups = new LinkedHashMap<>();
        for (int i = 0; i < allRows.size(); i++) {
            final String key = groupKey(allRows.get(i), dupColIndices);
            List<Integer> indices = groups.get(key);
            if (indices == null) {
                indices = new ArrayList<>();
                groups.put(key, indices);
            }
            indices.add(i);
        }

        final Map<String, Integer> chosen = new LinkedHashMap<>();
        for (final Map.Entry<String, List<Integer>> entry : groups.entrySet()) {
            final List<Integer> indices = entry.getValue();
            int bestIdx = indices.get(0);
            if (orderColIdx >= 0 && indices.size() > 1) {
                for (final int idx : indices) {
                    if (shouldReplace(allRows.get(idx).get(orderColIdx),
                            allRows.get(bestIdx).get(orderColIdx), keepMin)) {
                        bestIdx = idx;
                    }
                }
            }
            chosen.put(entry.getKey(), bestIdx);
        }

        final List<Row> result = new ArrayList<>();
        for (int i = 0; i < allRows.size(); i++) {
            final Row row = allRows.get(i);
            final String key = groupKey(row, dupColIndices);
            final List<Integer> group = groups.get(key);

            final String status;
            if (group.size() == 1) {
                status = "unique";
            } else if (chosen.get(key).intValue() == i) {
                status = "chosen";
            } else {
                status = "duplicate";
            }

            final Object[] values = new Object[row.size() + 1];
            for (int j = 0; j < row.size(); j++) {
                values[j] = row.get(j);
            }
            values[row.size()] = status;
            result.add(RowFactory.create(values));
        }
        return result;
    }

    private String groupKey(final Row row, final int[] indices) {
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

    private static boolean calculateKeepMin(final String rowSelection, final String orderDirection) {
        if ("MINIMUM".equals(rowSelection)) {
            return true;
        }
        if ("MAXIMUM".equals(rowSelection)) {
            return false;
        }
        boolean ascending = "ASC".equals(orderDirection);
        if ("LAST".equals(rowSelection)) {
            ascending = !ascending;
        }
        return ascending;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean shouldReplace(final Object newVal, final Object existingVal, final boolean keepMin) {
        if (newVal == null) return false;
        if (existingVal == null) return true;
        final int cmp = (newVal instanceof Comparable)
            ? ((Comparable) newVal).compareTo(existingVal)
            : newVal.toString().compareTo(existingVal.toString());
        return keepMin ? cmp < 0 : cmp > 0;
    }
}
