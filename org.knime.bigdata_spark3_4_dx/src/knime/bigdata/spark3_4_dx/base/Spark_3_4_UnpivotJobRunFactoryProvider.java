package knime.bigdata.spark3_4_dx.base;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

import knime.bigdata.spark3_4_dx.jobs.unpivot.UnpivotJobFactory;

/**
 * Registers Spark 3.4 job factories (DX custom jobs).
 */
public class Spark_3_4_UnpivotJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    public Spark_3_4_UnpivotJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
              new UnpivotJobFactory());
    }
}
