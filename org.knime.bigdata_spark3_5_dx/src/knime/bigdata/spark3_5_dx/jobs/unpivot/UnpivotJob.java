package knime.bigdata.spark3_5_dx.jobs.unpivot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import knime.bigdata.spark_dx.node.unpivot.SparkUnpivotJobInput;
import knime.bigdata.spark_dx.node.unpivot.SparkUnpivotJobOutput;

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
        final String[] retainedCols = safe(input.getRetainedColumns());

        if (valueCols.length == 0) {
            throw new KNIMESparkException("No value columns configured for unpivot.");
        }

        final boolean skipMissing = input.isSkipRowsContainingMissingCells();
        final String outNameCol = nonEmpty(input.getOutputNameColumn(), "ColumnNames");
        final String outValueCol = nonEmpty(input.getOutputValueColumn(), "ColumnValues");

        validateColumnsExist(inputFrame, retainedCols, "retained");
        validateColumnsExist(inputFrame, valueCols, "value");

        final Set<String> overlap = new HashSet<>();
        for (String r : retainedCols) overlap.add(r);
        for (String v : valueCols) {
            if (overlap.contains(v)) {
                throw new KNIMESparkException("Value column overlaps retained column: " + v);
            }
        }

        final String stackExpr = buildStackExpr(valueCols, outNameCol, outValueCol);

        final List<String> selectExprs = new ArrayList<>(retainedCols.length + 1);
        for (String c : retainedCols) {
            selectExprs.add(quoteCol(c));
        }
        selectExprs.add(stackExpr);

        Dataset<Row> result = inputFrame.selectExpr(selectExprs.toArray(new String[0]));

        if (skipMissing) {
            result = result.filter(quoteCol(outValueCol) + " IS NOT NULL");
        }

        namedObjects.addDataFrame(namedOutputObject, result);

        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkUnpivotJobOutput(namedOutputObject, outputSchema);
    }

    private static String buildStackExpr(final String[] valueCols, final String outNameCol, final String outValueCol) {
        final StringBuilder sb = new StringBuilder();
        sb.append("stack(").append(valueCols.length);
        for (String c : valueCols) {
            sb.append(", '").append(escapeSingleQuotes(c)).append("', ");
            sb.append("cast(").append(quoteCol(c)).append(" as string)");
        }
        sb.append(") as (").append(quoteCol(outNameCol)).append(", ").append(quoteCol(outValueCol)).append(")");
        return sb.toString();
    }

    private static void validateColumnsExist(final Dataset<Row> df, final String[] cols, final String kind) throws KNIMESparkException {
        if (cols == null || cols.length == 0) return;

        final Set<String> existing = new HashSet<>();
        for (String c : df.columns()) existing.add(c);

        for (String c : cols) {
            if (!existing.contains(c)) {
                throw new KNIMESparkException("Unknown " + kind + " column: " + c);
            }
        }
    }

    private static String[] safe(final String[] arr) {
        return arr == null ? new String[0] : arr;
    }

    private static String nonEmpty(final String v, final String def) {
        return (v == null || v.trim().isEmpty()) ? def : v.trim();
    }

    private static String quoteCol(final String col) {
        final String escaped = col.replace("`", "``");
        return "`" + escaped + "`";
    }

    private static String escapeSingleQuotes(final String s) {
        return s.replace("'", "''");
    }
}
