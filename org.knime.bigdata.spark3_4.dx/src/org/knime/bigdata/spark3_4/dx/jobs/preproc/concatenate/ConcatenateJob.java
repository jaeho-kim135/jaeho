package org.knime.bigdata.spark3_4.dx.jobs.preproc.concatenate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.concatenate.SparkConcatenateJobInput;
import org.knime.bigdata.spark.dx.node.preproc.concatenate.SparkConcatenateJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

/**
 * Spark job that vertically concatenates two DataFrames using collectAsList + createDataFrame.
 * Uses the Livy/JDK8 safe pattern (no UNION ALL, no DataFrame.union()).
 * Output column order follows the mapping array order (= config table order).
 */
@SparkClass
public class ConcatenateJob
    implements SparkJob<SparkConcatenateJobInput, SparkConcatenateJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkConcatenateJobOutput runJob(final SparkContext sparkContext,
            final SparkConcatenateJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        // 1. Load both DataFrames
        final Dataset<Row> leftDF = namedObjects.getDataFrame(input.getLeftInputObject());
        final Dataset<Row> rightDF = namedObjects.getDataFrame(input.getRightInputObject());

        final String[] leftArr = input.getLeftColumns();
        final String[] rightArr = input.getRightColumns();

        final StructType leftSchema = leftDF.schema();
        final StructType rightSchema = rightDF.schema();

        // Build name sets for defensive column existence checks
        final Set<String> leftColNames = new HashSet<>(Arrays.asList(leftSchema.fieldNames()));
        final Set<String> rightColNames = new HashSet<>(Arrays.asList(rightSchema.fieldNames()));

        // 2. Build output schema — follow mapping array order (= config table order)
        final List<StructField> outputFields = new ArrayList<>();
        final Set<String> usedOutputNames = new LinkedHashSet<>();
        // Per output column: 'M'=mapped, 'L'=unmatched left, 'R'=unmatched right
        final List<Character> colTypes = new ArrayList<>();
        final List<String> colLeftSrc = new ArrayList<>();  // source left column name
        final List<String> colRightSrc = new ArrayList<>(); // source right column name

        for (int i = 0; i < leftArr.length; i++) {
            final String left = leftArr[i];
            final String right = (i < rightArr.length) ? rightArr[i] : "";
            // Check both non-empty AND actually exists in the Spark schema
            final boolean hasLeft = left != null && !left.isEmpty() && leftColNames.contains(left);
            final boolean hasRight = right != null && !right.isEmpty() && rightColNames.contains(right);

            if (hasLeft && hasRight) {
                // Mapped pair — resolve type, handle duplicate output names
                final String outputName = makeUnique(left, usedOutputNames);
                final DataType leftType = leftSchema.apply(left).dataType();
                final DataType rightType = rightSchema.apply(right).dataType();
                final DataType resolvedType = resolveTypePair(leftType, rightType);
                outputFields.add(DataTypes.createStructField(outputName, resolvedType, true));
                usedOutputNames.add(outputName);
                colTypes.add('M');
                colLeftSrc.add(left);
                colRightSrc.add(right);
            } else if (hasLeft) {
                if ("FILL_NULL".equals(input.getUnmatchedLeftAction())) {
                    final String outputName = makeUnique(left, usedOutputNames);
                    outputFields.add(DataTypes.createStructField(
                        outputName, leftSchema.apply(left).dataType(),
                        leftSchema.apply(left).nullable()));
                    usedOutputNames.add(outputName);
                    colTypes.add('L');
                    colLeftSrc.add(left);
                    colRightSrc.add("");
                }
            } else if (hasRight) {
                if ("FILL_NULL".equals(input.getUnmatchedRightAction())) {
                    final String outputName = makeUnique(right, usedOutputNames);
                    outputFields.add(DataTypes.createStructField(
                        outputName, rightSchema.apply(right).dataType(), true));
                    usedOutputNames.add(outputName);
                    colTypes.add('R');
                    colLeftSrc.add("");
                    colRightSrc.add(right);
                }
            }
        }

        final StructType outputSchema = DataTypes.createStructType(outputFields);

        // 3. Collect rows (Livy/JDK8 safe pattern)
        final List<Row> leftRows = leftDF.collectAsList();
        final List<Row> rightRows = rightDF.collectAsList();
        final List<Row> allRows = new ArrayList<>(leftRows.size() + rightRows.size());

        // 3a. Transform left rows
        for (final Row leftRow : leftRows) {
            final Object[] values = new Object[outputFields.size()];
            for (int idx = 0; idx < outputFields.size(); idx++) {
                final char ct = colTypes.get(idx);
                if (ct == 'M') {
                    try {
                        final int srcIdx = leftSchema.fieldIndex(colLeftSrc.get(idx));
                        values[idx] = convertValueToOutputType(
                            leftRow.get(srcIdx),
                            leftSchema.apply(colLeftSrc.get(idx)).dataType(),
                            outputFields.get(idx).dataType());
                    } catch (final Exception e) {
                        values[idx] = null;
                    }
                } else if (ct == 'L') {
                    try {
                        final int srcIdx = leftSchema.fieldIndex(colLeftSrc.get(idx));
                        values[idx] = leftRow.get(srcIdx);
                    } catch (final Exception e) {
                        values[idx] = null;
                    }
                } else {
                    values[idx] = null;
                }
            }
            allRows.add(RowFactory.create(values));
        }

        // 3b. Transform right rows
        for (final Row rightRow : rightRows) {
            final Object[] values = new Object[outputFields.size()];
            for (int idx = 0; idx < outputFields.size(); idx++) {
                final char ct = colTypes.get(idx);
                if (ct == 'M') {
                    try {
                        final int srcIdx = rightSchema.fieldIndex(colRightSrc.get(idx));
                        final Object rightVal = rightRow.get(srcIdx);
                        if (rightVal == null) {
                            values[idx] = null;
                        } else {
                            values[idx] = convertRightValue(rightVal,
                                leftSchema.apply(colLeftSrc.get(idx)).dataType(),
                                rightSchema.apply(colRightSrc.get(idx)).dataType(),
                                outputFields.get(idx).dataType());
                        }
                    } catch (final Exception e) {
                        values[idx] = null;
                    }
                } else if (ct == 'L') {
                    values[idx] = null;
                } else {
                    try {
                        final int srcIdx = rightSchema.fieldIndex(colRightSrc.get(idx));
                        values[idx] = rightRow.get(srcIdx);
                    } catch (final Exception e) {
                        values[idx] = null;
                    }
                }
            }
            allRows.add(RowFactory.create(values));
        }

        // 4. Create result DataFrame
        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final Dataset<Row> result = spark.createDataFrame(allRows, outputSchema);

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSpec = TypeConverters.convertSpec(result.schema());
        return new SparkConcatenateJobOutput(namedOutputObject, outputSpec);
    }

    // ── Type resolution (pair-based) ─────────────────────────────────────────

    private DataType resolveTypePair(final DataType leftType, final DataType rightType) {
        if (leftType.equals(rightType)) {
            return leftType;
        }
        if (isNumericType(leftType) && isNumericType(rightType)) {
            final List<DataType> rights = new ArrayList<>();
            rights.add(rightType);
            return promoteNumericTypes(leftType, rights);
        }
        if (isDateType(leftType) && isDateType(rightType)) {
            return (leftType instanceof TimestampType || rightType instanceof TimestampType)
                ? DataTypes.TimestampType : DataTypes.DateType;
        }
        // Any side is String, or category mismatch → String
        return DataTypes.StringType;
    }

    private Object convertRightValue(final Object rightVal, final DataType leftType,
            final DataType rightType, final DataType outputType) {
        if (leftType.equals(rightType)) {
            return rightVal;
        }
        if (isNumericType(leftType) && isNumericType(rightType)) {
            return convertNumeric(rightVal, outputType);
        }
        if (isDateType(leftType) && isDateType(rightType)) {
            return convertDate(rightVal, outputType);
        }
        // Mismatch → toString
        return rightVal.toString();
    }

    private Object convertValueToOutputType(final Object value, final DataType sourceType,
            final DataType outputType) {
        if (value == null) {
            return null;
        }
        if (sourceType.equals(outputType)) {
            return value;
        }
        if (isNumericType(sourceType) && isNumericType(outputType)) {
            return convertNumeric(value, outputType);
        }
        if (isDateType(sourceType) && isDateType(outputType)) {
            return convertDate(value, outputType);
        }
        if (outputType instanceof StringType) {
            return value.toString();
        }
        return value;
    }

    // ── Utility methods ──────────────────────────────────────────────────────

    private String makeUnique(final String name, final Set<String> usedNames) {
        if (!usedNames.contains(name)) {
            return name;
        }
        int suffix = 1;
        while (usedNames.contains(name + "(" + suffix + ")")) {
            suffix++;
        }
        return name + "(" + suffix + ")";
    }

    private boolean isNumericType(final DataType type) {
        return type instanceof IntegerType || type instanceof LongType
            || type instanceof DoubleType || type instanceof FloatType
            || type instanceof ShortType || type instanceof ByteType;
    }

    private boolean isDateType(final DataType type) {
        return type instanceof DateType || type instanceof TimestampType;
    }

    private DataType promoteNumericTypes(final DataType left, final List<DataType> rights) {
        final List<DataType> allTypes = new ArrayList<>();
        allTypes.add(left);
        allTypes.addAll(rights);

        if (allTypes.stream().anyMatch(t -> t instanceof DoubleType || t instanceof FloatType)) {
            return DataTypes.DoubleType;
        }
        if (allTypes.stream().anyMatch(t -> t instanceof LongType)) {
            return DataTypes.LongType;
        }
        return DataTypes.IntegerType;
    }

    private Object convertNumeric(final Object value, final DataType targetType) {
        if (value == null) {
            return null;
        }
        final Number num = (Number) value;
        if (targetType instanceof DoubleType) {
            return num.doubleValue();
        }
        if (targetType instanceof LongType) {
            return num.longValue();
        }
        if (targetType instanceof FloatType) {
            return num.floatValue();
        }
        if (targetType instanceof IntegerType) {
            return num.intValue();
        }
        if (targetType instanceof ShortType) {
            return num.shortValue();
        }
        return value;
    }

    private Object convertDate(final Object value, final DataType targetType) {
        if (value == null) {
            return null;
        }
        if (targetType instanceof TimestampType && value instanceof java.sql.Date) {
            return new java.sql.Timestamp(((java.sql.Date) value).getTime());
        }
        return value;
    }
}
