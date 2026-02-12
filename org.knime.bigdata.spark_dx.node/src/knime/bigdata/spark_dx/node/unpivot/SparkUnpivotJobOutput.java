package knime.bigdata.spark_dx.node.unpivot;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

public final class SparkUnpivotJobOutput extends JobOutput implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String[] m_namedOutputObjects;
    private final IntermediateSpec m_outputSchema;

    public SparkUnpivotJobOutput(final String namedOutputObject, final IntermediateSpec outputSchema) {
        m_namedOutputObjects = new String[] { namedOutputObject };
        m_outputSchema = outputSchema;
    }

    public String getFirstNamedOutputObject() {
        return m_namedOutputObjects[0];
    }

    public IntermediateSpec getOutputSchema() {
        return m_outputSchema;
    }
}
