package org.knime.bigdata.spark3_5.dx.jobs.preproc.rounddouble;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.rounddouble.SparkRoundDoubleJobInput;
import org.knime.bigdata.spark.dx.node.preproc.rounddouble.SparkRoundDoubleJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.ceil;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.floor;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;

/**
 * Spark job that rounds numeric columns using configurable rounding methods.
 * Supports decimal places, significant digits, and integer modes with various
 * rounding strategies including HALF_UP, CEILING, FLOOR, UP, DOWN, HALF_DOWN, and HALF_EVEN.
 */
@SparkClass
public class RoundDoubleJob implements SparkJob<SparkRoundDoubleJobInput, SparkRoundDoubleJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkRoundDoubleJobOutput runJob(final SparkContext sparkContext, final SparkRoundDoubleJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputDF = namedObjects.getDataFrame(namedInputObject);

        final String[] columns = input.getColumns();
        final String numberMode = input.getNumberMode();
        final int precision = "INTEGER".equals(numberMode) ? 0 : input.getPrecision();
        final boolean isReplace = input.isReplace();
        final String suffix = input.getSuffix();

        // Determine Java RoundingMode
        final RoundingMode rm;
        if ("HALF_AWAY_FROM_ZERO".equals(input.getRoundingStandard())) {
            rm = RoundingMode.HALF_UP;
        } else {
            switch (input.getRoundingAdvanced()) {
                case "AWAY_FROM_ZERO":
                    rm = RoundingMode.UP;
                    break;
                case "TOWARDS_ZERO":
                    rm = RoundingMode.DOWN;
                    break;
                case "TO_LARGER":
                    rm = RoundingMode.CEILING;
                    break;
                case "TO_SMALLER":
                    rm = RoundingMode.FLOOR;
                    break;
                case "HALF_TOWARDS_ZERO":
                    rm = RoundingMode.HALF_DOWN;
                    break;
                case "HALF_TO_EVEN":
                    rm = RoundingMode.HALF_EVEN;
                    break;
                default:
                    rm = RoundingMode.UP;
                    break;
            }
        }

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        Dataset<Row> result = inputDF;

        for (final String colName : columns) {
            final Column c = col("`" + colName + "`");
            Column rounded;

            if ("SIGNIFICANT_DIGITS".equals(numberMode)) {
                // UDF for significant digits rounding
                final int sigFigs = precision;
                final RoundingMode finalRm = rm;
                final String udfName = "__round_sig_" + colName.replaceAll("[^a-zA-Z0-9]", "_") + "__";
                spark.udf().register(udfName,
                    (UDF2<Double, Integer, Double>) (val, sf) -> {
                        if (val == null || val.isNaN() || val.isInfinite()) {
                            return val;
                        }
                        final BigDecimal bd = BigDecimal.valueOf(val)
                            .round(new MathContext(sf, finalRm));
                        return bd.doubleValue();
                    }, DataTypes.DoubleType);
                rounded = callUDF(udfName, c, lit(sigFigs));
            } else {
                // DECIMALS or INTEGER mode
                switch (rm.name()) {
                    case "HALF_UP":
                        rounded = round(c, precision);
                        break;
                    case "CEILING": {
                        final double factor = Math.pow(10, precision);
                        rounded = ceil(c.multiply(lit(factor))).divide(lit(factor));
                        break;
                    }
                    case "FLOOR": {
                        final double factor = Math.pow(10, precision);
                        rounded = floor(c.multiply(lit(factor))).divide(lit(factor));
                        break;
                    }
                    case "UP": {
                        // AWAY_FROM_ZERO: positive -> ceil, negative -> floor
                        final double factor = Math.pow(10, precision);
                        rounded = expr("CASE WHEN `" + colName + "` >= 0 "
                            + "THEN ceil(`" + colName + "` * " + factor + ") / " + factor
                            + " ELSE floor(`" + colName + "` * " + factor + ") / " + factor + " END");
                        break;
                    }
                    case "DOWN": {
                        // TOWARDS_ZERO: positive -> floor, negative -> ceil
                        final double factor = Math.pow(10, precision);
                        rounded = expr("CASE WHEN `" + colName + "` >= 0 "
                            + "THEN floor(`" + colName + "` * " + factor + ") / " + factor
                            + " ELSE ceil(`" + colName + "` * " + factor + ") / " + factor + " END");
                        break;
                    }
                    default: {
                        // HALF_DOWN, HALF_EVEN: use UDF with BigDecimal
                        final RoundingMode finalRm2 = rm;
                        final String udfName = "__round_dec_" + colName.replaceAll("[^a-zA-Z0-9]", "_") + "__";
                        spark.udf().register(udfName,
                            (UDF2<Double, Integer, Double>) (val, p) -> {
                                if (val == null || val.isNaN() || val.isInfinite()) {
                                    return val;
                                }
                                return BigDecimal.valueOf(val).setScale(p, finalRm2).doubleValue();
                            }, DataTypes.DoubleType);
                        rounded = callUDF(udfName, c, lit(precision));
                        break;
                    }
                }
            }

            // INTEGER mode: cast result to LongType
            if ("INTEGER".equals(numberMode)) {
                rounded = rounded.cast(DataTypes.LongType);
            }

            final String outputCol = isReplace ? colName : colName + suffix;
            result = result.withColumn(outputCol, rounded);
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkRoundDoubleJobOutput(namedOutputObject, outputSchema);
    }
}
