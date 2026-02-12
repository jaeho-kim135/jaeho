package knime.bigdata.spark3_5_dx.jobs.unpivot;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;

import knime.bigdata.spark_dx.node.unpivot.SparkUnpivotJobInput;
import knime.bigdata.spark_dx.node.unpivot.SparkUnpivotJobOutput;
import knime.bigdata.spark_dx.node.unpivot.SparkUnpivotNodeModel;

/**
 * Unpivot job factory for Spark 3.5.
 */
public class UnpivotJobFactory extends DefaultJobRunFactory<SparkUnpivotJobInput, SparkUnpivotJobOutput> {

    public UnpivotJobFactory() {
        super(SparkUnpivotNodeModel.JOB_ID, UnpivotJob.class, SparkUnpivotJobOutput.class);
    }
}
