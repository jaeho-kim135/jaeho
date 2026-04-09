package org.knime.bigdata.spark.dx.node.preproc.topk;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
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
                .map(tableSpec -> tableSpec.stream().collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }
    }

    // ── SORT CRITERION (ArrayWidget element) ─────────────────────────────────

    /**
     * A single sort criterion consisting of a column and a sort order.
     * Used as the element type of the {@code @ArrayWidget} sorting criteria array.
     */
    static class SortCriterion implements NodeParameters {
        @Widget(title = "Column", description = "The column to sort by.")
        @ChoicesProvider(SparkColumnChoicesProvider.class)
        String m_column = "";

        @Widget(title = "Order", description = "The sort order for this column.")
        @ValueSwitchWidget
        SortOrder m_order = SortOrder.DESCENDING;

        /** Default constructor. */
        SortCriterion() {}

        /** Constructor with values. */
        SortCriterion(final String column, final SortOrder order) {
            m_column = column;
            m_order = order;
        }
    }

    // ── CUSTOM PERSISTORS ────────────────────────────────────────────────────

    /**
     * Bridges SortCriterion[] to/from parallel String arrays (sortColumns[], sortOrders[]).
     * Also handles backward-compatible loading from the old fixed sortColumn1/sortColumn2 format.
     */
    static final class SortCriteriaPersistor implements NodeParametersPersistor<SortCriterion[]> {

        @Override
        public SortCriterion[] load(final NodeSettingsRO settings) throws InvalidSettingsException {
            // New format: sortColumns[] + sortOrders[] arrays
            if (settings.containsKey(SparkTopKRowFilterSettings.CFG_SORT_COLUMNS)) {
                final String[] columns = settings.getStringArray(SparkTopKRowFilterSettings.CFG_SORT_COLUMNS);
                final String[] orders = settings.getStringArray(
                    SparkTopKRowFilterSettings.CFG_SORT_ORDERS, new String[0]);
                final SortCriterion[] criteria = new SortCriterion[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    final String orderStr = (i < orders.length) ? orders[i] : "DESCENDING";
                    SortOrder order;
                    try {
                        order = SortOrder.valueOf(orderStr);
                    } catch (final IllegalArgumentException e) {
                        order = SortOrder.DESCENDING;
                    }
                    criteria[i] = new SortCriterion(columns[i], order);
                }
                return criteria.length > 0 ? criteria : new SortCriterion[]{new SortCriterion()};
            }

            // Backward compat: old format with sortColumn1/sortColumn2
            if (settings.containsKey(SparkTopKRowFilterSettings.CFG_SORT_COLUMN1_LEGACY)) {
                final String col1 = settings.getString(SparkTopKRowFilterSettings.CFG_SORT_COLUMN1_LEGACY, "");
                final String ord1 = settings.getString(SparkTopKRowFilterSettings.CFG_SORT_ORDER1_LEGACY, "DESCENDING");
                final boolean useSecond = settings.getBoolean(
                    SparkTopKRowFilterSettings.CFG_USE_SECOND_SORT_LEGACY, false);

                SortOrder order1;
                try { order1 = SortOrder.valueOf(ord1); } catch (final IllegalArgumentException e) {
                    order1 = SortOrder.DESCENDING;
                }

                if (useSecond) {
                    final String col2 = settings.getString(
                        SparkTopKRowFilterSettings.CFG_SORT_COLUMN2_LEGACY, "");
                    final String ord2 = settings.getString(
                        SparkTopKRowFilterSettings.CFG_SORT_ORDER2_LEGACY, "DESCENDING");
                    SortOrder order2;
                    try { order2 = SortOrder.valueOf(ord2); } catch (final IllegalArgumentException e) {
                        order2 = SortOrder.DESCENDING;
                    }
                    if (!col2.isEmpty()) {
                        return new SortCriterion[]{
                            new SortCriterion(col1, order1),
                            new SortCriterion(col2, order2)
                        };
                    }
                }
                return new SortCriterion[]{new SortCriterion(col1, order1)};
            }

            return new SortCriterion[]{new SortCriterion()};
        }

        @Override
        public void save(final SortCriterion[] obj, final NodeSettingsWO settings) {
            final SortCriterion[] criteria = (obj != null) ? obj : new SortCriterion[0];
            final String[] columns = new String[criteria.length];
            final String[] orders = new String[criteria.length];
            for (int i = 0; i < criteria.length; i++) {
                columns[i] = criteria[i].m_column != null ? criteria[i].m_column : "";
                orders[i] = criteria[i].m_order != null ? criteria[i].m_order.name() : "DESCENDING";
            }
            settings.addStringArray(SparkTopKRowFilterSettings.CFG_SORT_COLUMNS, columns);
            settings.addStringArray(SparkTopKRowFilterSettings.CFG_SORT_ORDERS, orders);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{
                {SparkTopKRowFilterSettings.CFG_SORT_COLUMNS},
                {SparkTopKRowFilterSettings.CFG_SORT_ORDERS}
            };
        }
    }

    /**
     * Persistor for group columns ColumnFilter using NameFilterConfiguration format.
     */
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
    @Widget(title = "Sorting criteria",
        description = "Define one or more sorting criteria for ranking. "
            + "Each criterion specifies a column and its sort order.")
    @ArrayWidget(elementTitle = "Sort Criterion", addButtonText = "Add Column", showSortButtons = true)
    @Persistor(SortCriteriaPersistor.class)
    SortCriterion[] m_sortingCriteria = new SortCriterion[]{new SortCriterion()};

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
