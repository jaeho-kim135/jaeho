package org.knime.bigdata.spark3_5.dx.jobs.preproc.caseconvert;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the case convert job run factory for Spark 3.5.
 */
public class CaseConvertJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public CaseConvertJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new CaseConvertJobRunFactory());
    }
}
