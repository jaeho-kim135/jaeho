package org.knime.bigdata.spark3_5.dx.jobs.preproc.colcombine;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Column Combiner job run factory for Spark 3.5.
 */
public class ColumnCombinerJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ColumnCombinerJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new ColumnCombinerJobRunFactory());
    }
}
