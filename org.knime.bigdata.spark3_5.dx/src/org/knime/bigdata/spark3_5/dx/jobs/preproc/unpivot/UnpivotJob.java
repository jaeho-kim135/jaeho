package org.knime.bigdata.spark3_5.dx.jobs.preproc.unpivot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobInput;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

/**
 * Spark job that performs unpivot using per-column simple SELECTs.
 * Each value column is queried individually (same pattern as MultiQueryJob),
 * results are collected to the driver and combined into a clean DataFrame.
 * This avoids UNION ALL, generator functions (stack/explode), and all
 * row-multiplying Spark operations for Livy/JDK8 compatibility.
 */
@SparkClass
public class UnpivotJob implements SparkJob<SparkUnpivotJobInput, SparkUnpivotJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkUnpivotJobOutput runJob(final SparkContext sparkContext, final SparkUnpivotJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] retainedColumns = input.getRetainedColumns();
        final String[] valueColumns = input.getValueColumns();
        final String variableColName = input.getVariableColName();
        final String valueColName = input.getValueColName();
        final boolean skipMissing = input.skipMissingValues();
        final boolean castToString = input.castToString();
        final boolean validateOnly = input.isValidateOnly();
        final String sortOption = input.getSortOption();

        // Build variable value map
        final Map<String, String> varMap = new HashMap<>();
        final String[] mapKeys = input.getVarMapKeys();
        final String[] mapVals = input.getVarMapValues();
        for (int i = 0; i < Math.min(mapKeys.length, mapVals.length); i++) {
            if (mapVals[i] != null && !mapVals[i].isEmpty()) {
                varMap.put(mapKeys[i], mapVals[i]);
            }
        }

        if (valueColumns.length == 0) {
            throw new KNIMESparkException("No value columns specified for unpivoting.");
        }

        final String tempView = "unpivot_" + UUID.randomUUID().toString().replace("-", "");

        try {
            inputFrame.createOrReplaceTempView(tempView);

            if (validateOnly) {
                // Validation: simple SELECT for first value column with LIMIT 5
                // Same pattern as MultiQueryJob validation (simple SELECT FROM view)
                final String testSql = buildSingleColumnSql(tempView, retainedColumns,
                    valueColumns[0], variableColName, valueColName, castToString,
                    skipMissing, varMap) + " LIMIT 5";
                final Dataset<Row> testResult = spark.sql(testSql);
                final String preview = testResult.showString(5, 20, false);
                final SparkUnpivotJobOutput output = new SparkUnpivotJobOutput(null, null);
                output.setPreviewData(preview);
                return output;
            }

            // Execute: run simple SELECT per value column, collect to driver, combine
            final String namedOutputObject = input.getFirstNamedOutputObject();
            final List<Row> allRows = new ArrayList<>();
            StructType outputSchema = null;

            for (int i = 0; i < valueColumns.length; i++) {
                final String sql = buildSingleColumnSql(tempView, retainedColumns,
                    valueColumns[i], variableColName, valueColName, castToString,
                    skipMissing, varMap);
                final Dataset<Row> part = spark.sql(sql);
                if (outputSchema == null) {
                    outputSchema = part.schema();
                }
                allRows.addAll(part.collectAsList());
            }

            // Create clean materialized DataFrame (no lazy plans, no view dependencies)
            Dataset<Row> result = spark.createDataFrame(allRows, outputSchema);

            // Apply sorting if requested — materialize to guarantee sort order in named object
            if ("retained".equals(sortOption) && retainedColumns.length > 0) {
                if (retainedColumns.length == 1) {
                    result = result.sort(retainedColumns[0]);
                } else {
                    final String[] rest = new String[retainedColumns.length - 1];
                    System.arraycopy(retainedColumns, 1, rest, 0, rest.length);
                    result = result.sort(retainedColumns[0], rest);
                }
                final List<Row> sortedRows = result.collectAsList();
                result = spark.createDataFrame(sortedRows, outputSchema);
            } else if ("variable".equals(sortOption)) {
                result = result.sort(variableColName);
                final List<Row> sortedRows = result.collectAsList();
                result = spark.createDataFrame(sortedRows, outputSchema);
            }

            namedObjects.addDataFrame(namedOutputObject, result);
            final IntermediateSpec spec = TypeConverters.convertSpec(result.schema());
            return new SparkUnpivotJobOutput(namedOutputObject, spec);

        } finally {
            spark.catalog().dropTempView(tempView);
        }
    }

    /**
     * Build a simple SELECT for a single value column (same pattern as MultiQueryJob):
     * SELECT `r1`, `r2`, 'label' AS `variable`, `col` AS `value` FROM view
     * [WHERE `col` IS NOT NULL]
     */
    private static String buildSingleColumnSql(final String tempView, final String[] retainedColumns,
            final String valueColumn, final String variableColName, final String valueColName,
            final boolean castToString, final boolean skipMissing, final Map<String, String> varMap) {

        final String label = varMap.containsKey(valueColumn)
            ? varMap.get(valueColumn) : valueColumn;

        final StringBuilder sql = new StringBuilder("SELECT ");
        for (final String rc : retainedColumns) {
            sql.append("`").append(rc).append("`, ");
        }
        sql.append("'").append(esc(label)).append("' AS `").append(variableColName).append("`, ");
        if (castToString) {
            sql.append("CAST(`").append(valueColumn).append("` AS STRING) AS `").append(valueColName).append("`");
        } else {
            sql.append("`").append(valueColumn).append("` AS `").append(valueColName).append("`");
        }
        sql.append(" FROM ").append(tempView);

        if (skipMissing) {
            sql.append(" WHERE `").append(valueColumn).append("` IS NOT NULL");
        }

        return sql.toString();
    }

    /** Escape single quotes for SQL string literals (SQL standard: '' not \'). */
    private static String esc(final String s) {
        return s.replace("'", "''");
    }
}
