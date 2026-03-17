package org.knime.bigdata.spark3_5.dx.jobs.preproc.stringreplacer;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the string replacer job run factory for Spark 3.5.
 */
public class StringReplacerJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringReplacerJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new StringReplacerJobRunFactory());
    }
}
