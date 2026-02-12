package knime.bigdata.spark3_5_dx.base;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

import knime.bigdata.spark3_5_dx.jobs.unpivot.UnpivotJobFactory;

/**
 * Registers Spark 3.5 job factories (DX custom jobs).
 */
public class Spark_3_5_UnpivotJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    public Spark_3_5_UnpivotJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
              new UnpivotJobFactory());
    }
}
