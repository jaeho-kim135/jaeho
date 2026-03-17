package org.knime.bigdata.spark3_4.dx.jobs.preproc.cellsplit;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the Cell Splitter job run factory for Spark 3.4.
 */
public class CellSplitterJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public CellSplitterJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new CellSplitterJobRunFactory());
    }
}
