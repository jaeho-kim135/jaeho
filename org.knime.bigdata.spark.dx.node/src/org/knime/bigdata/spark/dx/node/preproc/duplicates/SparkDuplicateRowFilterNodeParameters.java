package org.knime.bigdata.spark.dx.node.preproc.duplicates;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.defaultdialog.setting.filter.util.ManualFilter;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;

/**
 * Node parameters (WebUI dialog settings) for the Spark Duplicate Row Filter node.
 */
@SuppressWarnings("restriction")
class SparkDuplicateRowFilterNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Duplicate Detection",
            description = "Select columns used to detect duplicate rows.")
        interface DuplicateDetectionSection {}

        @Section(title = "Row Selection",
            description = "Select which row to keep from each duplicate group.")
        @After(DuplicateDetectionSection.class)
        interface RowSelectionSection {}

        @Section(title = "Order Column",
            description = "Column used to determine row ordering within duplicate groups.")
        @After(RowSelectionSection.class)
        interface OrderColumnSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    /** Kept for backward compatibility with old workflows that may have KEEP stored. */
    enum DuplicateHandling {
        REMOVE, KEEP;
    }

    enum RowSelection {
        @Label("Keep first occurrence") FIRST,
        @Label("Keep last occurrence") LAST,
        @Label("Keep row with minimum value") MINIMUM,
        @Label("Keep row with maximum value") MAXIMUM,
        @Label("Remove all duplicates") REMOVE_ALL;
    }

    enum OrderDirection {
        @Label("Ascending") ASC,
        @Label("Descending") DESC;
    }

    // ── PARAMETER REFERENCES ─────────────────────────────────────────────────

    interface RowSelRef extends ParameterReference<RowSelection> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    /** Shows the order column when row selection requires ordering (FIRST/LAST/MIN/MAX). */
    static final class NeedsOrderColumnPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(RowSelRef.class).isOneOf(
                RowSelection.FIRST, RowSelection.LAST,
                RowSelection.MINIMUM, RowSelection.MAXIMUM);
        }
    }

    /** Shows the order direction only for FIRST/LAST modes. */
    static final class IsFirstOrLastPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(RowSelRef.class).isOneOf(RowSelection.FIRST, RowSelection.LAST);
        }
    }

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    static final class SparkAllColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }
    }

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkDuplicateRowFilterSettings.CFG_COLUMNS;

        ColumnsPersistor() {
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

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Duplicate Detection ──────────────────────────────────────────────────
    @Layout(DialogSections.DuplicateDetectionSection.class)
    @Widget(title = "Duplicate Detection Columns",
        description = "Columns used to identify duplicate rows. "
            + "Rows with identical values in all selected columns are considered duplicates.")
    @ColumnFilterWidget(choicesProvider = SparkAllColumnChoicesProvider.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // Hidden: persisted for backward compat but not shown in UI (always REMOVE)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_DUPLICATE_HANDLING)
    DuplicateHandling m_duplicateHandling = DuplicateHandling.REMOVE;

    // ── Row Selection ────────────────────────────────────────────────────────
    @Layout(DialogSections.RowSelectionSection.class)
    @Widget(title = "Row Selection",
        description = "Select which row to keep from each group of duplicates.")
    @RadioButtonsWidget
    @ValueReference(RowSelRef.class)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_ROW_SELECTION)
    RowSelection m_rowSelection = RowSelection.FIRST;

    // ── Order Column (visible for FIRST/LAST/MIN/MAX only) ───────────────────
    @Layout(DialogSections.OrderColumnSection.class)
    @Widget(title = "Order Column",
        description = "Column used to determine row ordering within each duplicate group. "
            + "Required for first/last/minimum/maximum row selection.")
    @ChoicesProvider(SparkAllColumnChoicesProvider.class)
    @Effect(predicate = NeedsOrderColumnPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_ORDER_COLUMN)
    String m_orderColumn = "";

    // ── Order Direction (visible for FIRST/LAST only) ────────────────────────
    @Layout(DialogSections.OrderColumnSection.class)
    @Widget(title = "Order Direction",
        description = "Sort direction for the order column when selecting first or last occurrence.")
    @ValueSwitchWidget
    @Effect(predicate = IsFirstOrLastPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_ORDER_DIRECTION)
    OrderDirection m_orderDirection = OrderDirection.ASC;

    // ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    SparkDuplicateRowFilterNodeParameters() {}

    // ── HELPERS ───────────────────────────────────────────────────────────────

    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) {
            return new String[0];
        }
        ManualFilter mf = filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) {
            return new String[0];
        }
        return mf.m_manuallySelected;
    }
}
