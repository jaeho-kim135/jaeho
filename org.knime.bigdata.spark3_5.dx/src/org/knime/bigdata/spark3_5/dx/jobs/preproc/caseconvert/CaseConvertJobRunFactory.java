package org.knime.bigdata.spark3_5.dx.jobs.preproc.caseconvert;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.caseconvert.SparkCaseConvertJobInput;
import org.knime.bigdata.spark.dx.node.preproc.caseconvert.SparkCaseConvertJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.caseconvert.SparkCaseConvertNodeModel;

/**
 * Case convert job run factory for Spark 3.5.
 */
public class CaseConvertJobRunFactory
    extends DefaultJobRunFactory<SparkCaseConvertJobInput, SparkCaseConvertJobOutput> {

    /** Constructor. */
    public CaseConvertJobRunFactory() {
        super(SparkCaseConvertNodeModel.JOB_ID,
            CaseConvertJob.class,
            SparkCaseConvertJobOutput.class);
    }
}
