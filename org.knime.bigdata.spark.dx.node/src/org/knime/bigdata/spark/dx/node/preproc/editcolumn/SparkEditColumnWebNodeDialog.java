package org.knime.bigdata.spark.dx.node.preproc.editcolumn;

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
 * WebUI dialog for the Spark Edit Column node.
 * Provides a modern HTML-based dialog with column editing table,
 * inline renaming, type casting dropdown, and drag-based reordering.
 */
@SuppressWarnings("restriction")
final class SparkEditColumnWebNodeDialog implements NodeDialog {

    private static final String BASE = "/js-src/dist/";

    @Override
    public Page getPage() {
        return Page.create()
            .fromString(() -> SparkEditColumnWebNodeDialog.class.getResourceAsStream(BASE + "spark-editcolumn.html"))
            .relativePath("spark-editcolumn.html")
            .addResource(() -> SparkEditColumnWebNodeDialog.class.getResourceAsStream(BASE + "assets/spark-editcolumn.js"), "assets/spark-editcolumn.js")
            .addResource(() -> SparkEditColumnWebNodeDialog.class.getResourceAsStream(BASE + "assets/spark-editcolumn.css"), "assets/spark-editcolumn.css")
            .addResource(() -> SparkEditColumnWebNodeDialog.class.getResourceAsStream(BASE + "assets/index.js"), "assets/index.js")
            .addResource(() -> SparkEditColumnWebNodeDialog.class.getResourceAsStream(BASE + "assets/index.css"), "assets/index.css")
            .addResource(() -> SparkEditColumnWebNodeDialog.class.getResourceAsStream(BASE + "assets/modulepreload-polyfill.js"), "assets/modulepreload-polyfill.js");
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
            .addDataSupplier("inputColumns", () -> getPortColumns(workflowControl));

        return new ScriptingNodeSettingsService(
            SparkEditColumnWebSettings::new,
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

    // ── Initial data supplier ──────────────────────────────────────────────

    private static Object getPortColumns(final WorkflowControl workflowControl) {
        try {
            final WorkflowControl.InputPortInfo[] inputInfo = workflowControl.getInputInfo();
            if (inputInfo == null || inputInfo.length == 0) {
                return Collections.emptyList();
            }

            // DefaultSparkNodeFactory prepends a hidden Spark context port.
            // Scan all ports for SparkDataPortObjectSpec.
            for (final WorkflowControl.InputPortInfo info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final PortObjectSpec portSpec = info.portSpec();
                if (portSpec instanceof SparkDataPortObjectSpec) {
                    return buildColumnList(((SparkDataPortObjectSpec) portSpec).getTableSpec());
                }
            }
            // Fallback: try DataTableSpec
            for (final WorkflowControl.InputPortInfo info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final PortObjectSpec portSpec = info.portSpec();
                if (portSpec instanceof DataTableSpec) {
                    return buildColumnList((DataTableSpec) portSpec);
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
