package org.knime.bigdata.spark3_4.dx.jobs.preproc.caseconvert;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.caseconvert.SparkCaseConvertJobInput;
import org.knime.bigdata.spark.dx.node.preproc.caseconvert.SparkCaseConvertJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.initcap;

/**
 * Spark job that converts the case of selected string columns using
 * UPPER(), LOWER(), or INITCAP() functions.
 */
@SparkClass
public class CaseConvertJob
    implements SparkJob<SparkCaseConvertJobInput, SparkCaseConvertJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkCaseConvertJobOutput runJob(final SparkContext sparkContext,
            final SparkCaseConvertJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final Dataset<Row> inputFrame = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final String[] columns = input.getColumns();
        final String mode = input.getMode();

        Dataset<Row> result = inputFrame;
        for (String colName : columns) {
            Column c = col("`" + colName + "`");
            Column converted;
            switch (mode) {
                case "LOWERCASE":
                    converted = lower(c);
                    break;
                case "TITLE_CASE":
                    converted = initcap(c);
                    break;
                case "UPPERCASE":
                default:
                    converted = upper(c);
                    break;
            }
            result = result.withColumn(colName, converted);
        }

        if (input.isValidateOnly()) {
            SparkCaseConvertJobOutput output = new SparkCaseConvertJobOutput(null, null);
            output.setPreviewData(result.showString(5, 20, false));
            return output;
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkCaseConvertJobOutput(namedOutputObject, outputSchema);
    }
}
