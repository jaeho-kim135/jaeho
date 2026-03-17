package org.knime.bigdata.spark3_5.dx.jobs.preproc.cellsplit;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the Cell Splitter job run factory for Spark 3.5.
 */
public class CellSplitterJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public CellSplitterJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new CellSplitterJobRunFactory());
    }
}
