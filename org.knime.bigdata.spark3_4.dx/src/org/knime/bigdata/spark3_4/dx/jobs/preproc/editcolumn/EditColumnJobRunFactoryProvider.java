package org.knime.bigdata.spark3_4.dx.jobs.preproc.editcolumn;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the edit column job run factory for Spark 3.4.
 */
public class EditColumnJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public EditColumnJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new EditColumnJobRunFactory());
    }
}
