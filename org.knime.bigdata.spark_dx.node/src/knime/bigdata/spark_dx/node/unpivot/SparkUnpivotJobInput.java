package knime.bigdata.spark_dx.node.unpivot;

import java.io.Serializable;
import java.util.Arrays;

import org.knime.bigdata.spark.core.job.JobInput;

/**
 * Job input sent to Spark side (serialized).
 * Keep it POJO + Serializable.
 */
public final class SparkUnpivotJobInput extends JobInput implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String[] m_namedInputObjects;
    private final String[] m_namedOutputObjects;

    private final String[] m_valueColumns;
    private final String[] m_retainedColumns;
    private final boolean m_skipMissingCells;

    public SparkUnpivotJobInput(
        final String namedInputObject,
        final String namedOutputObject,
        final String[] valueColumns,
        final String[] retainedColumns,
        final boolean skipMissingCells
    ) {
        m_namedInputObjects = new String[] { namedInputObject };
        m_namedOutputObjects = new String[] { namedOutputObject };
        m_valueColumns = valueColumns != null ? valueColumns : new String[0];
        m_retainedColumns = retainedColumns != null ? retainedColumns : new String[0];
        m_skipMissingCells = skipMissingCells;
    }

    public String getFirstNamedInputObject() {
        return m_namedInputObjects[0];
    }

    public String getFirstNamedOutputObject() {
        return m_namedOutputObjects[0];
    }

    public String[] getValueColumns() {
        return m_valueColumns;
    }

    public String[] getRetainedColumns() {
        return m_retainedColumns;
    }

    public boolean isSkipMissingCells() {
        return m_skipMissingCells;
    }

    @Override
    public String toString() {
        return "SparkUnpivotJobInput{value=" + Arrays.toString(m_valueColumns)
            + ", retained=" + Arrays.toString(m_retainedColumns)
            + ", skipMissing=" + m_skipMissingCells + "}";
    }
}
