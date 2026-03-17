package org.knime.bigdata.spark3_4.dx.jobs.preproc.caseconvert;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the case convert job run factory for Spark 3.4.
 */
public class CaseConvertJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public CaseConvertJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new CaseConvertJobRunFactory());
    }
}
