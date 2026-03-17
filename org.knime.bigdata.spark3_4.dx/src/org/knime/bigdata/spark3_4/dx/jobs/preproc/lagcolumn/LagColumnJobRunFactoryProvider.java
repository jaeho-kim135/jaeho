package org.knime.bigdata.spark3_4.dx.jobs.preproc.lagcolumn;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

public class LagColumnJobRunFactoryProvider extends DefaultJobRunFactoryProvider {
    public LagColumnJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE, new LagColumnJobRunFactory());
    }
}
