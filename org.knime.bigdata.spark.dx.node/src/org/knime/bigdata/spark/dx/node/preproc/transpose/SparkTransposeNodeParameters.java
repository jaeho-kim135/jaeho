package org.knime.bigdata.spark.dx.node.preproc.transpose;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.message.TextMessage;

/**
 * Node parameters (WebUI dialog settings) for the Spark Table Transposer node.
 * Provides settings for ID column selection and maximum row limit.
 */
@SuppressWarnings("restriction")
class SparkTransposeNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Settings",
            description = "Configure transpose options.")
        interface SettingsSection {}

        @Section(title = "Warning")
        @After(SettingsSection.class)
        interface WarningSection {}
    }

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    static final class SparkColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }
    }

    // ── WARNING PROVIDER ─────────────────────────────────────────────────────

    /**
     * Static warning message provider that always shows a warning about data collection to driver.
     */
    static final class WarningProvider implements StateProvider<Optional<TextMessage.Message>> {

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            return Optional.of(new TextMessage.Message(
                "Transpose collects all data to the driver. "
                    + "Only suitable for small datasets (< 100,000 rows).",
                "", TextMessage.MessageType.WARNING));
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "ID Column (optional)",
        description = "Column to use as row headers in the transposed output. "
            + "If not selected, row numbers (Row0, Row1, ...) will be used as column names.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Persist(configKey = SparkTransposeSettings.CFG_ID_COLUMN)
    String m_idColumn = "";

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "Maximum rows",
        description = "Safety limit for the number of rows to transpose. "
            + "If the input exceeds this limit, the node will fail with an error. "
            + "This prevents accidentally collecting very large datasets to the driver.")
    @NumberInputWidget
    @Persist(configKey = SparkTransposeSettings.CFG_MAX_ROWS)
    int m_maxRows = 1000;

    @Layout(DialogSections.WarningSection.class)
    @TextMessage(WarningProvider.class)
    Void m_warning;

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkTransposeNodeParameters() {
    }
}
