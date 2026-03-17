package org.knime.bigdata.spark3_5.dx.jobs.preproc.rowsplitter;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Row Splitter job run factory for Spark 3.5.
 */
public class RowSplitterJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public RowSplitterJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new RowSplitterJobRunFactory());
    }
}
