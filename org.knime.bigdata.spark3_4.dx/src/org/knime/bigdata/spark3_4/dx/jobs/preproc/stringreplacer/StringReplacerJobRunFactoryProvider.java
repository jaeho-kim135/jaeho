package org.knime.bigdata.spark3_4.dx.jobs.preproc.stringreplacer;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the string replacer job run factory for Spark 3.4.
 */
public class StringReplacerJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public StringReplacerJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new StringReplacerJobRunFactory());
    }
}
