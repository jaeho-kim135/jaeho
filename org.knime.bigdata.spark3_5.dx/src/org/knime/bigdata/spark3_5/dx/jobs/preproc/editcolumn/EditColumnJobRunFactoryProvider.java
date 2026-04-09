package org.knime.bigdata.spark3_5.dx.jobs.preproc.editcolumn;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the edit column job run factory for Spark 3.5.
 */
public class EditColumnJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public EditColumnJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new EditColumnJobRunFactory());
    }
}
