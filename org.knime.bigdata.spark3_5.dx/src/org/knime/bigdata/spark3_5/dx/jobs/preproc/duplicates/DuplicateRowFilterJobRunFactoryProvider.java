package org.knime.bigdata.spark3_5.dx.jobs.preproc.duplicates;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Duplicate Row Filter job run factory for Spark 3.5.
 */
public class DuplicateRowFilterJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public DuplicateRowFilterJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new DuplicateRowFilterJobRunFactory());
    }
}
