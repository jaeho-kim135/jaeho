package org.knime.bigdata.spark3_4.dx.jobs.preproc.colcombine;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Column Combiner job run factory for Spark 3.4.
 */
public class ColumnCombinerJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ColumnCombinerJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new ColumnCombinerJobRunFactory());
    }
}
