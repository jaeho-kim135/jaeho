package org.knime.bigdata.spark.dx.node.preproc.numbertostring;

import java.util.List;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Number to String node.
 */
@SuppressWarnings("restriction")
class SparkNumberToStringNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the numeric columns to convert to String.")
        interface ColumnSelectionSection {}
    }

    // ── COLUMN CHOICES PROVIDER ──────────────────────────────────────────────

    /**
     * Provides only numeric columns (DoubleValue compatible) from the Spark input spec.
     */
    static final class SparkNumericColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream()
                    .filter(colSpec -> colSpec.getType().isCompatible(DoubleValue.class))
                    .toList())
                .orElse(List.of());
        }
    }

    // ── PERSISTOR ────────────────────────────────────────────────────────────

    static final class IncludedColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkNumberToStringSettings.CFG_INCLUDE;

        IncludedColumnsPersistor() {
            super(KEY);
        }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return loadColumnFilterWithFallback(settings, KEY);
        }
    }

    private static ColumnFilter loadColumnFilterWithFallback(final NodeSettingsRO settings,
            final String key) throws InvalidSettingsException {
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey("included_names")) {
                return LegacyColumnFilterPersistor.load(settings, key);
            }
            final String[] incl = sub.getStringArray("InclList", new String[0]);
            return buildColumnFilterFromNames(incl, key);
        } catch (final InvalidSettingsException e) {
            return new ColumnFilter();
        }
    }

    private static ColumnFilter buildColumnFilterFromNames(final String[] included, final String key)
            throws InvalidSettingsException {
        final NodeSettings temp = new NodeSettings("_temp");
        final NodeSettingsWO sub = temp.addNodeSettings(key);
        sub.addString("filter-type", "STANDARD");
        sub.addStringArray("included_names", included);
        sub.addStringArray("excluded_names", new String[0]);
        sub.addString("enforce_option", "EnforceInclusion");
        return LegacyColumnFilterPersistor.load(temp, key);
    }

    // ── FIELDS ───────────────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnSelectionSection.class)
    @Widget(title = "Included Columns",
        description = "Numeric columns to convert to String.")
    @ColumnFilterWidget(choicesProvider = SparkNumericColumnChoicesProvider.class)
    @Persistor(IncludedColumnsPersistor.class)
    ColumnFilter m_inclCols = new ColumnFilter();

    // ── CONSTRUCTOR ──────────────────────────────────────────────────────────

    SparkNumberToStringNodeParameters() {}

    // ── HELPERS ──────────────────────────────────────────────────────────────

    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) return new String[0];
        final org.knime.core.webui.node.dialog.defaultdialog.setting.filter.util.ManualFilter mf =
            filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) return new String[0];
        return mf.m_manuallySelected;
    }
}
