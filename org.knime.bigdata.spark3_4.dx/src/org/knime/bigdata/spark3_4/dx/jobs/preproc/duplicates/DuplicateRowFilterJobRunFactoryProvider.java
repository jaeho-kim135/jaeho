package org.knime.bigdata.spark3_4.dx.jobs.preproc.duplicates;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Duplicate Row Filter job run factory for Spark 3.4.
 */
public class DuplicateRowFilterJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public DuplicateRowFilterJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new DuplicateRowFilterJobRunFactory());
    }
}
