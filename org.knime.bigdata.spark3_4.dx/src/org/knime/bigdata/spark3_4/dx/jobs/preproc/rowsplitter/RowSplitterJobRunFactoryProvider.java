package org.knime.bigdata.spark3_4.dx.jobs.preproc.rowsplitter;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Row Splitter job run factory for Spark 3.4.
 */
public class RowSplitterJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public RowSplitterJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new RowSplitterJobRunFactory());
    }
}
