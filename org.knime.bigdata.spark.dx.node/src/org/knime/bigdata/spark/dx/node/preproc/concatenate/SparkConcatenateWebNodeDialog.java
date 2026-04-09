package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.webui.data.RpcDataService;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.page.Page;
import org.knime.scripting.editor.GenericInitialDataBuilder;
import org.knime.scripting.editor.ScriptingNodeSettingsService;
import org.knime.scripting.editor.WorkflowControl;

/**
 * WebUI dialog for the Spark Concatenate node.
 * Provides a modern HTML-based dialog with column mapping table,
 * auto-mapping, and unmatched column handling.
 */
@SuppressWarnings("restriction")
final class SparkConcatenateWebNodeDialog implements NodeDialog {

    private static final String BASE = "/js-src/dist/";

    @Override
    public Page getPage() {
        return Page.create()
            .fromString(() -> SparkConcatenateWebNodeDialog.class.getResourceAsStream(BASE + "spark-concatenate.html"))
            .relativePath("spark-concatenate.html")
            .addResource(() -> SparkConcatenateWebNodeDialog.class.getResourceAsStream(BASE + "assets/spark-concatenate.js"), "assets/spark-concatenate.js")
            .addResource(() -> SparkConcatenateWebNodeDialog.class.getResourceAsStream(BASE + "assets/spark-concatenate.css"), "assets/spark-concatenate.css")
            .addResource(() -> SparkConcatenateWebNodeDialog.class.getResourceAsStream(BASE + "assets/index.js"), "assets/index.js")
            .addResource(() -> SparkConcatenateWebNodeDialog.class.getResourceAsStream(BASE + "assets/index.css"), "assets/index.css")
            .addResource(() -> SparkConcatenateWebNodeDialog.class.getResourceAsStream(BASE + "assets/modulepreload-polyfill.js"), "assets/modulepreload-polyfill.js");
    }

    @Override
    public Set<SettingsType> getSettingsTypes() {
        return Collections.singleton(SettingsType.MODEL);
    }

    @Override
    public NodeSettingsService getNodeSettingsService() {
        final WorkflowControl workflowControl = new WorkflowControl(NodeContext.getContext().getNodeContainer());

        final GenericInitialDataBuilder initialDataBuilder = GenericInitialDataBuilder
            .createDefaultInitialDataBuilder(NodeContext.getContext())
            .addDataSupplier("leftColumns", () -> getPortColumns(workflowControl, 0))
            .addDataSupplier("rightColumns", () -> getPortColumns(workflowControl, 1));

        return new ScriptingNodeSettingsService(
            SparkConcatenateWebSettings::new,
            initialDataBuilder
        );
    }

    @Override
    public Optional<RpcDataService> createRpcDataService() {
        return Optional.empty();
    }

    @Override
    public boolean canBeEnlarged() {
        return true;
    }

    // ── Initial data suppliers ──────────────────────────────────────────────

    private static Object getPortColumns(final WorkflowControl workflowControl, final int targetPortIndex) {
        try {
            final WorkflowControl.InputPortInfo[] inputInfo = workflowControl.getInputInfo();
            if (inputInfo == null || inputInfo.length == 0) {
                return Collections.emptyList();
            }

            // DefaultSparkNodeFactory prepends a hidden Spark context port.
            // Scan all ports for SparkDataPortObjectSpec, tracking user-port index.
            int sparkPortIndex = 0;
            for (final WorkflowControl.InputPortInfo info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final PortObjectSpec portSpec = info.portSpec();
                if (portSpec instanceof SparkDataPortObjectSpec) {
                    if (sparkPortIndex == targetPortIndex) {
                        return buildColumnList(((SparkDataPortObjectSpec) portSpec).getTableSpec());
                    }
                    sparkPortIndex++;
                }
            }
            // Fallback: try DataTableSpec
            int tablePortIndex = 0;
            for (final WorkflowControl.InputPortInfo info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final PortObjectSpec portSpec = info.portSpec();
                if (portSpec instanceof DataTableSpec) {
                    if (tablePortIndex == targetPortIndex) {
                        return buildColumnList((DataTableSpec) portSpec);
                    }
                    tablePortIndex++;
                }
            }
        } catch (final Exception e) {
            // Return empty list on error
        }
        return Collections.emptyList();
    }

    private static List<Map<String, String>> buildColumnList(final DataTableSpec spec) {
        final List<Map<String, String>> columns = new ArrayList<>();
        for (int i = 0; i < spec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = spec.getColumnSpec(i);
            final Map<String, String> col = new LinkedHashMap<>();
            col.put("name", colSpec.getName());
            col.put("type", colSpec.getType().getName());
            columns.add(col);
        }
        return columns;
    }
}
