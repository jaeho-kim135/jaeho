package knime.bigdata.spark3_4_dx.jobs.unpivot;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import knime.bigdata.spark_dx.node.unpivot.SparkUnpivotJobInput;
import knime.bigdata.spark_dx.node.unpivot.SparkUnpivotJobOutput;

/**
 * Spark-side Unpivot job (Spark 3.4).
 *
 * - Avoids calling optional getters that may not exist on SparkUnpivotJobInput
 * - Avoids Dataset.selectExpr(...) to prevent scala.collection.Seq missing-type compile errors
 * - Uses explode(array(struct(...))) pattern instead of stack/selectExpr
 */
@SparkClass
public class UnpivotJob implements SparkJob<SparkUnpivotJobInput, SparkUnpivotJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkUnpivotJobOutput runJob(final SparkContext sparkContext,
                                        final SparkUnpivotJobInput input,
                                        final NamedObjects namedObjects) throws Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final String namedOutputObject = input.getFirstNamedOutputObject();

        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] valueCols = safe(input.getValueColumns());
        String[] retainedCols = safe(input.getRetainedColumns());

        if (valueCols.length == 0) {
            throw new KNIMESparkException("No value columns configured for unpivot.");
        }

        // ---- OPTIONAL options (read via reflection if methods exist; otherwise use defaults) ----
        final boolean skipMissing = readBoolean(input, false,
                "isSkipRowsContainingMissingCells",
                "getSkipRowsContainingMissingCells",
                "isSkipMissing",
                "getSkipMissing");

        final String outNameCol = nonEmpty(readString(input, null,
                "getOutputNameColumn",
                "getOutputNameCol",
                "getNameColumn"), "ColumnNames");

        final String outValueCol = nonEmpty(readString(input, null,
                "getOutputValueColumn",
                "getOutputValueCol",
                "getValueColumn"), "ColumnValues");

        // ---- If retainedCols not specified, keep all non-value columns ----
        if (retainedCols.length == 0) {
            retainedCols = computeRetainedCols(inputFrame.columns(), valueCols);
        }

        // ---- Validate columns existence & overlap ----
        validateColumnsExist(inputFrame, retainedCols, "retained");
        validateColumnsExist(inputFrame, valueCols, "value");

        final Set<String> retainedSet = new HashSet<>();
        for (String r : retainedCols) retainedSet.add(r);

        for (String v : valueCols) {
            if (retainedSet.contains(v)) {
                throw new KNIMESparkException("Value column overlaps retained column: " + v);
            }
        }

        // Prevent collision with existing columns
        final Set<String> existing = new HashSet<>();
        for (String c : inputFrame.columns()) existing.add(c);

        if (existing.contains(outNameCol) || existing.contains(outValueCol)) {
            throw new KNIMESparkException("Output column name already exists in input: "
                    + (existing.contains(outNameCol) ? outNameCol : outValueCol)
                    + " (change output column names in settings or JobInput defaults)");
        }

        // ---- Build unpivot using explode(array(struct(...))) ----
        final List<Column> structs = new ArrayList<>(valueCols.length);
        for (String c : valueCols) {
            // struct( lit("colName") as outNameCol, cast(col(c) as string) as outValueCol )
            structs.add(
                functions.struct(
                    functions.lit(c).alias(outNameCol),
                    colRef(c).cast("string").alias(outValueCol)
                )
            );
        }

        Dataset<Row> df = inputFrame.withColumn("__kv",
                functions.explode(functions.array(structs.toArray(new Column[0]))));

        // Promote struct fields to top-level columns
        df = df.withColumn(outNameCol, functions.col("__kv").getField(outNameCol));
        df = df.withColumn(outValueCol, functions.col("__kv").getField(outValueCol));

        // Drop helper column
        df = df.drop("__kv");

        // Drop value columns (original wide columns)
        for (String c : valueCols) {
            df = df.drop(colRef(c)); // Column-based drop to handle special chars safely
        }

        // If user configured retainedCols as subset, drop everything not in keep set
        final Set<String> keep = new HashSet<>();
        for (String r : retainedCols) keep.add(r);
        keep.add(outNameCol);
        keep.add(outValueCol);

        for (String c : df.columns()) {
            if (!keep.contains(c)) {
                df = df.drop(colRef(c));
            }
        }

        if (skipMissing) {
            df = df.filter(colRef(outValueCol).isNotNull());
        }

        namedObjects.addDataFrame(namedOutputObject, df);

        final IntermediateSpec outputSchema = TypeConverters.convertSpec(df.schema());
        return new SparkUnpivotJobOutput(namedOutputObject, outputSchema);
    }

    // ---------------- helpers ----------------

    private static String[] safe(final String[] arr) {
        return arr == null ? new String[0] : arr;
    }

    private static String nonEmpty(final String v, final String def) {
        return (v == null || v.trim().isEmpty()) ? def : v.trim();
    }

    private static void validateColumnsExist(final Dataset<Row> df, final String[] cols, final String kind)
            throws KNIMESparkException {
        if (cols == null || cols.length == 0) return;

        final Set<String> existing = new HashSet<>();
        for (String c : df.columns()) existing.add(c);

        for (String c : cols) {
            if (!existing.contains(c)) {
                throw new KNIMESparkException("Unknown " + kind + " column: " + c);
            }
        }
    }

    private static String[] computeRetainedCols(final String[] allCols, final String[] valueCols) {
        final Set<String> values = new HashSet<>();
        for (String v : valueCols) values.add(v);

        final List<String> retained = new ArrayList<>();
        for (String c : allCols) {
            if (!values.contains(c)) retained.add(c);
        }
        return retained.toArray(new String[0]);
    }

    /**
     * Robust column reference for names containing spaces/dots/special chars:
     * uses expr(`...`) quoting to avoid nested-field interpretation.
     */
    private static Column colRef(final String colName) {
        return functions.expr(quoteIdentifier(colName));
    }

    private static String quoteIdentifier(final String colName) {
        if (colName == null) return "``";
        // escape backticks by doubling
        final String escaped = colName.replace("`", "``");
        return "`" + escaped + "`";
    }

    // ---- reflection: read optional getters without compile dependency ----

    private static Boolean readBoolean(final Object target, final Boolean def, final String... noArgMethodNames) {
        final Object v = invokeFirstNoArg(target, noArgMethodNames);
        if (v instanceof Boolean) return (Boolean)v;
        if (v instanceof String) return Boolean.parseBoolean(((String)v).trim());
        return def;
    }

    private static String readString(final Object target, final String def, final String... noArgMethodNames) {
        final Object v = invokeFirstNoArg(target, noArgMethodNames);
        return v == null ? def : String.valueOf(v);
    }

    private static Object invokeFirstNoArg(final Object target, final String... methodNames) {
        if (target == null || methodNames == null) return null;
        final Class<?> cls = target.getClass();

        for (String name : methodNames) {
            if (name == null || name.isEmpty()) continue;
            try {
                final Method m = cls.getMethod(name);
                m.setAccessible(true);
                return m.invoke(target);
            } catch (Exception ignore) {
                // try next
            }
        }
        return null;
    }
}
