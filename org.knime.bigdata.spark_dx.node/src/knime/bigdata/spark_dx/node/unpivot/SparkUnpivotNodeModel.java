package knime.bigdata.spark_dx.node.unpivot;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 * Spark Unpivot node model (phase-1: passthrough).
 *
 * Phase-2:
 *  - Create SparkUnpivotJobFactory in spark3_4 & spark3_5 plugin
 *  - Replace execute() to submit job and return new SparkDataPortObject
 */
public final class SparkUnpivotNodeModel extends NodeModel {

    /** Must be unique across Spark jobs (when you later add spark job plugins). */
    public static final String JOB_ID = "spark_dx.unpivot";

    /** Spark port type (Spark Data in/out). */
    private static final PortType SPARK_DATA_PORT =
        PortTypeRegistry.getInstance().getPortType(SparkDataPortObject.class);

    private final SparkUnpivotSettings m_settings = new SparkUnpivotSettings();

    /** Mandatory constructor: NodeModel has no default ctor. */
    protected SparkUnpivotNodeModel() {
        super(new PortType[] { SPARK_DATA_PORT }, new PortType[] { SPARK_DATA_PORT });
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        // Basic validation only (settings-level)
        if (m_settings.getValueColumns() == null || m_settings.getValueColumns().length == 0) {
            throw new InvalidSettingsException("Select at least one value column to unpivot.");
        }

        // Check overlap (settings-only)
        final Set<String> v = new HashSet<>(Arrays.asList(m_settings.getValueColumns()));
        final Set<String> r = new HashSet<>(Arrays.asList(m_settings.getRetainedColumns()));
        v.retainAll(r);
        if (!v.isEmpty()) {
            throw new InvalidSettingsException("Value columns and retained columns overlap: " + v);
        }

        // Phase-1 passthrough: output spec == input spec
        return new PortObjectSpec[] { inSpecs[0] };
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        // Phase-1 passthrough
        // TODO Phase-2: build SparkUnpivotJobInput and run via Spark job registry/context util
        return new PortObject[] { inObjects[0] };
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveTo(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validate(settings);
    }

    @Override
    protected void reset() {
        // no-op
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // no-op
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // no-op
    }
}
