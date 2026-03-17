package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringmanip;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.stringmanip.SparkStringManipJobInput;
import org.knime.bigdata.spark.dx.node.preproc.stringmanip.SparkStringManipJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.expr;

/**
 * Spark job that applies a single SQL string expression to produce a new or replaced column.
 * Uses Spark's {@code withColumn(name, expr(sql))} to apply the expression.
 */
@SparkClass
public class StringManipJob implements SparkJob<SparkStringManipJobInput, SparkStringManipJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkStringManipJobOutput runJob(final SparkContext sparkContext, final SparkStringManipJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String expression = input.getExpression();
        if (expression == null || expression.trim().isEmpty()) {
            throw new KNIMESparkException("Expression cannot be empty.");
        }
        final boolean validateOnly = input.isValidateOnly();

        // Determine the output column name
        final String outputColName;
        if (input.isReplace()) {
            outputColName = input.getReplaceColumn();
        } else {
            outputColName = input.getOutputColName();
        }
        if (outputColName == null || outputColName.trim().isEmpty()) {
            throw new KNIMESparkException("Output column name cannot be empty.");
        }

        if (validateOnly) {
            return runValidation(inputFrame, expression, outputColName);
        }

        return runExecution(input, namedObjects, inputFrame, expression, outputColName);
    }

    private SparkStringManipJobOutput runValidation(final Dataset<Row> inputFrame,
            final String expression, final String outputColName)
            throws KNIMESparkException {

        try {
            final Dataset<Row> result = inputFrame.withColumn(outputColName, expr(expression));

            // Generate preview with first 5 rows
            final String preview = result.showString(5, 20, false);
            final SparkStringManipJobOutput output = new SparkStringManipJobOutput(null, null);
            output.setPreviewData(preview);
            return output;
        } catch (final Exception e) {
            throw new KNIMESparkException(
                "Expression (" + expression + ") failed: " + e.getMessage(), e);
        }
    }

    private SparkStringManipJobOutput runExecution(final SparkStringManipJobInput input,
            final NamedObjects namedObjects, final Dataset<Row> inputFrame,
            final String expression, final String outputColName)
            throws KNIMESparkException {

        final String namedOutputObject = input.getFirstNamedOutputObject();

        try {
            final Dataset<Row> result = inputFrame.withColumn(outputColName, expr(expression));

            namedObjects.addDataFrame(namedOutputObject, result);
            final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
            return new SparkStringManipJobOutput(namedOutputObject, outputSchema);
        } catch (final Exception e) {
            throw new KNIMESparkException(
                "Expression (" + expression + ") failed: " + e.getMessage(), e);
        }
    }
}
