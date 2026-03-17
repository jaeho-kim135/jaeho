package org.knime.bigdata.spark3_5.dx.jobs.preproc.numbertostring;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.numbertostring.SparkNumberToStringJobInput;
import org.knime.bigdata.spark.dx.node.preproc.numbertostring.SparkNumberToStringJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;

/**
 * Spark job that converts numeric columns to String type using DataFrame API.
 */
@SparkClass
public class NumberToStringJob implements SparkJob<SparkNumberToStringJobInput, SparkNumberToStringJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkNumberToStringJobOutput runJob(final SparkContext sparkContext,
            final SparkNumberToStringJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();

        Dataset<Row> result = inputFrame;

        for (final String colName : columns) {
            result = result.withColumn(colName, col(colName).cast(DataTypes.StringType));
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkNumberToStringJobOutput(namedOutputObject, outputSchema);
    }
}
