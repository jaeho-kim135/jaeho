package org.knime.bigdata.spark3_5.dx.jobs.preproc.lagcolumn;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

public class LagColumnJobRunFactoryProvider extends DefaultJobRunFactoryProvider {
    public LagColumnJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE, new LagColumnJobRunFactory());
    }
}
