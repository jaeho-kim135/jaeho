package org.knime.bigdata.spark3_5.dx.jobs.preproc.editcolumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.editcolumn.SparkEditColumnJobInput;
import org.knime.bigdata.spark.dx.node.preproc.editcolumn.SparkEditColumnJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;

/**
 * Spark job that renames, casts, and reorders columns using select().
 * Uses col().cast().as() pattern — no row multiplication, fully Livy/JDK8 compatible.
 */
@SparkClass
public class EditColumnJob
    implements SparkJob<SparkEditColumnJobInput, SparkEditColumnJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkEditColumnJobOutput runJob(final SparkContext sparkContext,
            final SparkEditColumnJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputDF = namedObjects.getDataFrame(namedInputObject);

        final String[] sourceColumns = input.getSourceColumns();
        final String[] newNames = input.getNewNames();
        final String[] newTypes = input.getNewTypes();

        // Build rename and cast maps
        final Set<String> editedOriginals = new LinkedHashSet<>();
        final Map<String, String> renameMap = new LinkedHashMap<>();
        final Map<String, String> castMap = new LinkedHashMap<>();

        for (int i = 0; i < sourceColumns.length; i++) {
            final String src = sourceColumns[i];
            editedOriginals.add(src);
            final String name = (i < newNames.length && newNames[i] != null && !newNames[i].isEmpty())
                ? newNames[i] : src;
            renameMap.put(src, name);
            final String newType = (i < newTypes.length) ? newTypes[i] : "KEEP";
            if (!"KEEP".equals(newType)) {
                castMap.put(src, newType);
            }
        }

        // Build select columns: edited columns in ArrayWidget order, then unedited in original order
        final List<String> originalColumns = Arrays.asList(inputDF.columns());
        final List<Column> selectCols = new ArrayList<>();

        // Edited columns
        for (final String src : sourceColumns) {
            Column c = col("`" + src.replace("`", "``") + "`");
            if (castMap.containsKey(src)) {
                c = castColumn(c, castMap.get(src));
            }
            c = c.as(renameMap.get(src));
            selectCols.add(c);
        }

        // Unedited columns in original order
        for (final String origCol : originalColumns) {
            if (!editedOriginals.contains(origCol)) {
                selectCols.add(col("`" + origCol.replace("`", "``") + "`"));
            }
        }

        final Dataset<Row> result = inputDF.select(selectCols.toArray(new Column[0]));

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkEditColumnJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * Casts a column to the specified target type.
     */
    private static Column castColumn(final Column c, final String targetType) {
        switch (targetType) {
            case "STRING":    return c.cast(DataTypes.StringType);
            case "INTEGER":   return c.cast(DataTypes.IntegerType);
            case "LONG":      return c.cast(DataTypes.LongType);
            case "DOUBLE":    return c.cast(DataTypes.DoubleType);
            case "FLOAT":     return c.cast(DataTypes.DoubleType);
            case "BOOLEAN":   return c.cast(DataTypes.BooleanType);
            case "DATE":      return c.cast(DataTypes.DateType);
            case "TIMESTAMP": return c.cast(DataTypes.TimestampType);
            default:          return c;
        }
    }
}
