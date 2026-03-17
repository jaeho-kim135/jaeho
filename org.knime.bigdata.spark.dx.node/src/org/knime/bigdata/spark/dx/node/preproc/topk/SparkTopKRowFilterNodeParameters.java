package org.knime.bigdata.spark.dx.node.preproc.topk;

import java.util.List;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.updates.util.BooleanReference;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Top k Row Filter node.
 */
@SuppressWarnings("restriction")
class SparkTopKRowFilterNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Filter Settings",
            description = "Configure the number of rows to select and the filter mode.")
        interface FilterSettingsSection {}

        @Section(title = "Sort Criteria",
            description = "Define the columns and sort orders for ranking.")
        @After(FilterSettingsSection.class)
        interface SortCriteriaSection {}

        @Section(title = "Group Columns",
            description = "Optional: select columns to perform top-k selection per group.")
        @After(SortCriteriaSection.class)
        interface GroupColumnsSection {}

        @Section(title = "Output Options",
            description = "Configure the output row ordering.")
        @After(GroupColumnsSection.class)
        interface OutputOptionsSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum FilterMode {
        @Label("Top k rows") ROWS,
        @Label("Top k unique values (all matching rows)") UNIQUE_VALUES;
    }

    enum OutputOrder {
        @Label("Sorted by ranking criteria") SORTED,
        @Label("Arbitrary (fastest)") ARBITRARY;
    }

    enum SortOrder {
        @Label("Descending") DESCENDING,
        @Label("Ascending") ASCENDING;
    }

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    static final class SparkColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().toList())
                .orElse(List.of());
        }
    }

    // ── EFFECT: show/hide second sort criterion ──────────────────────────────

    interface UseSecondSortRef extends BooleanReference {}

    static final class UseSecondSortPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getBoolean(UseSecondSortRef.class).isTrue();
        }
    }

    // ── PERSISTOR for group columns ─────────────────────────────────────────

    static final class GroupColumnsPersistor extends LegacyColumnFilterPersistor {
        GroupColumnsPersistor() {
            super(SparkTopKRowFilterSettings.CFG_GROUP_COLUMNS);
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Filter Settings ──────────────────────────────────────────────────────

    @Layout(DialogSections.FilterSettingsSection.class)
    @Widget(title = "Number of rows (k)",
        description = "The number of top rows (or unique values) to select. Must be at least 1.")
    @NumberInputWidget
    @Persist(configKey = "k")
    long m_k = 5;

    @Layout(DialogSections.FilterSettingsSection.class)
    @Widget(title = "Filter mode",
        description = "Choose whether to select the top k rows or all rows matching the top k unique sort-column values.")
    @ValueSwitchWidget
    @Persist(configKey = "filterMode")
    FilterMode m_filterMode = FilterMode.ROWS;

    // ── Sort Criteria ────────────────────────────────────────────────────────

    @Layout(DialogSections.SortCriteriaSection.class)
    @Widget(title = "Sort column",
        description = "The primary column to sort by for ranking.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Persist(configKey = "sortColumn1")
    String m_sortColumn1 = "";

    @Layout(DialogSections.SortCriteriaSection.class)
    @Widget(title = "Sort order",
        description = "The sort order for the primary sort column.")
    @ValueSwitchWidget
    @Persist(configKey = "sortOrder1")
    SortOrder m_sortOrder1 = SortOrder.DESCENDING;

    @Layout(DialogSections.SortCriteriaSection.class)
    @Widget(title = "Use second sort criterion",
        description = "Enable a second sort column for tie-breaking.")
    @ValueReference(UseSecondSortRef.class)
    @Persist(configKey = "useSecondSort")
    boolean m_useSecondSort = false;

    @Layout(DialogSections.SortCriteriaSection.class)
    @Widget(title = "Second sort column",
        description = "The secondary column to sort by for tie-breaking.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Effect(predicate = UseSecondSortPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = "sortColumn2")
    String m_sortColumn2 = "";

    @Layout(DialogSections.SortCriteriaSection.class)
    @Widget(title = "Second sort order",
        description = "The sort order for the secondary sort column.")
    @ValueSwitchWidget
    @Effect(predicate = UseSecondSortPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = "sortOrder2")
    SortOrder m_sortOrder2 = SortOrder.DESCENDING;

    @Layout(DialogSections.SortCriteriaSection.class)
    @Widget(title = "Put missing values at end",
        description = "If checked, null values are sorted to the end regardless of the sort order.")
    @Persist(configKey = "missingsToEnd")
    boolean m_missingsToEnd = true;

    // ── Group Columns ────────────────────────────────────────────────────────

    @Layout(DialogSections.GroupColumnsSection.class)
    @Widget(title = "Group columns",
        description = "Optional columns to group by. When specified, the top-k selection is performed "
            + "independently within each group using Spark window functions.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @Persistor(GroupColumnsPersistor.class)
    ColumnFilter m_groupColumns = new ColumnFilter();

    // ── Output Options ───────────────────────────────────────────────────────

    @Layout(DialogSections.OutputOptionsSection.class)
    @Widget(title = "Output order",
        description = "Choose whether the output should be sorted by the ranking criteria or left in arbitrary order (faster).")
    @ValueSwitchWidget
    @Persist(configKey = "outputOrder")
    OutputOrder m_outputOrder = OutputOrder.SORTED;

    // ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    SparkTopKRowFilterNodeParameters() {}
}
