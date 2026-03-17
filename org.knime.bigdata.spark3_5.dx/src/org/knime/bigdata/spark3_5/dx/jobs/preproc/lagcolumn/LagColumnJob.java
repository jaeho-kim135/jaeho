package org.knime.bigdata.spark3_5.dx.jobs.preproc.lagcolumn;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.lagcolumn.SparkLagColumnJobInput;
import org.knime.bigdata.spark.dx.node.preproc.lagcolumn.SparkLagColumnJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.lit;

/**
 * Spark 3.5 job that creates lag/lead columns using Window functions.
 */
@SparkClass
public class LagColumnJob implements SparkJob<SparkLagColumnJobInput, SparkLagColumnJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkLagColumnJobOutput runJob(final SparkContext sparkContext, final SparkLagColumnJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final Dataset<Row> inputFrame = namedObjects.getDataFrame(input.getFirstNamedInputObject());

        final String srcCol = input.getColumn();
        final String orderCol = input.getOrderColumn();
        final int numCopies = input.getNumCopies();
        final int interval = input.getLagInterval();
        final boolean isLag = "LAG".equals(input.getDirection());
        final String[] groupCols = input.getGroupColumns();
        final boolean skipIncomplete = input.isSkipIncompleteRows();

        // 1. Build WindowSpec with optional partitionBy + orderBy
        WindowSpec window;
        if (groupCols.length > 0) {
            final Column[] partCols = new Column[groupCols.length];
            for (int i = 0; i < groupCols.length; i++) {
                partCols[i] = col("`" + groupCols[i] + "`");
            }
            window = Window.partitionBy(partCols).orderBy(col("`" + orderCol + "`"));
        } else {
            window = Window.orderBy(col("`" + orderCol + "`"));
        }

        // 2. For each copy i=1..numCopies, add a lag/lead column
        Dataset<Row> result = inputFrame;
        final String sign = isLag ? "-" : "+";
        final String[] newColNames = new String[numCopies];

        for (int i = 1; i <= numCopies; i++) {
            final int offset = i * interval;
            final String newColName = srcCol + "(" + sign + offset + ")";
            newColNames[i - 1] = newColName;

            final Column lagCol;
            if (isLag) {
                lagCol = lag(col("`" + srcCol + "`"), offset).over(window);
            } else {
                lagCol = lead(col("`" + srcCol + "`"), offset).over(window);
            }
            result = result.withColumn(newColName, lagCol);
        }

        // 3. If skipIncomplete: filter rows where ALL lag columns are NOT null
        if (skipIncomplete) {
            Column notNullCondition = lit(true);
            for (final String lagColName : newColNames) {
                notNullCondition = notNullCondition.and(col("`" + lagColName + "`").isNotNull());
            }
            result = result.filter(notNullCondition);
        }

        if (input.isValidateOnly()) {
            final SparkLagColumnJobOutput output = new SparkLagColumnJobOutput(null, null);
            try {
                output.setPreviewData(result.showString(5, 20, false));
            } catch (final Exception e) {
                output.setPreviewData("Preview failed: " + e.getMessage());
            }
            return output;
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkLagColumnJobOutput(namedOutputObject, outputSchema);
    }
}
