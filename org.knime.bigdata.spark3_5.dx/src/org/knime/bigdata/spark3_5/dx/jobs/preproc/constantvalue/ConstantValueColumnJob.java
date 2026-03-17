package org.knime.bigdata.spark3_5.dx.jobs.preproc.constantvalue;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.constantvalue.SparkConstantValueColumnJobInput;
import org.knime.bigdata.spark.dx.node.preproc.constantvalue.SparkConstantValueColumnJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;

/**
 * Spark job that adds or replaces a column with a constant value.
 * Uses Spark's {@code lit()} function to create constant columns.
 */
@SparkClass
public class ConstantValueColumnJob
    implements SparkJob<SparkConstantValueColumnJobInput, SparkConstantValueColumnJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkConstantValueColumnJobOutput runJob(final SparkContext sparkContext,
            final SparkConstantValueColumnJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String colName = input.getColumnName();
        final String valueType = input.getValueType();
        final boolean isMissing = input.isMissing();

        // Build the constant column expression
        final Column constCol;
        if (isMissing) {
            constCol = createMissingColumn(valueType);
        } else {
            final String value = input.getValue();
            constCol = createValueColumn(valueType, value);
        }

        // Apply the constant column
        final Dataset<Row> result = inputFrame.withColumn(colName, constCol);

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkConstantValueColumnJobOutput(namedOutputObject, outputSchema);
    }

    /**
     * Creates a null literal column cast to the specified type.
     */
    private static Column createMissingColumn(final String valueType) throws KNIMESparkException {
        final Column nullLit = lit(null);
        switch (valueType) {
            case "STRING":
                return nullLit.cast(DataTypes.StringType);
            case "INTEGER":
                return nullLit.cast(DataTypes.IntegerType);
            case "LONG":
                return nullLit.cast(DataTypes.LongType);
            case "DOUBLE":
                return nullLit.cast(DataTypes.DoubleType);
            case "BOOLEAN":
                return nullLit.cast(DataTypes.BooleanType);
            case "DATE":
                return nullLit.cast(DataTypes.DateType);
            case "TIMESTAMP":
                return nullLit.cast(DataTypes.TimestampType);
            default:
                throw new KNIMESparkException("Unsupported value type: " + valueType);
        }
    }

    /**
     * Creates a constant value column with the specified type and value.
     */
    private static Column createValueColumn(final String valueType, final String value)
            throws KNIMESparkException {
        try {
            switch (valueType) {
                case "STRING":
                    return lit(value);
                case "INTEGER":
                    return lit(Integer.parseInt(value.trim()));
                case "LONG":
                    return lit(Long.parseLong(value.trim()));
                case "DOUBLE":
                    return lit(Double.parseDouble(value.trim()));
                case "BOOLEAN":
                    return lit(Boolean.parseBoolean(value.trim()));
                case "DATE":
                    return to_date(lit(value.trim()), "yyyy-MM-dd");
                case "TIMESTAMP":
                    return to_timestamp(lit(value.trim()), "yyyy-MM-dd HH:mm:ss");
                default:
                    throw new KNIMESparkException("Unsupported value type: " + valueType);
            }
        } catch (final NumberFormatException e) {
            throw new KNIMESparkException(
                "Cannot parse value '" + value + "' as " + valueType + ": " + e.getMessage(), e);
        }
    }
}
