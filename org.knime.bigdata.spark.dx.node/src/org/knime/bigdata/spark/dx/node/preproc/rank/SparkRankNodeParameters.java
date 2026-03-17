package org.knime.bigdata.spark.dx.node.preproc.rank;

import java.util.List;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
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
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Rank node.
 * Uses ArrayWidget for ranking criteria (column + order pairs) and
 * ColumnFilterWidget for group-by columns.
 */
@SuppressWarnings("restriction")
class SparkRankNodeParameters implements NodeParameters {

    // ── LAYOUT ───────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Ranking Columns",
            description = "Define the columns and sort orders used to compute the rank.")
        interface RankingColumnsSection {}

        @Section(title = "Group Columns",
            description = "Optional columns to partition (group) by. Ranks are computed separately within each group.")
        @After(RankingColumnsSection.class)
        interface GroupColumnsSection {}

        @Section(title = "Options",
            description = "Configure the ranking mode, output column name, and data type.")
        @After(GroupColumnsSection.class)
        interface OptionsSection {}
    }

    // ── ENUMS ────────────────────────────────────────────────────────────────

    enum SortOrder {
        @Label(value = "Ascending", description = "Rank from smallest to largest value.")
        ASCENDING,
        @Label(value = "Descending", description = "Rank from largest to smallest value.")
        DESCENDING;
    }

    enum RankMode {
        @Label(value = "Standard (1, 1, 3, 4...)",
            description = "Same rank for ties, then gap. Uses RANK() function.")
        STANDARD,
        @Label(value = "Dense (1, 1, 2, 3...)",
            description = "Same rank for ties, no gap. Uses DENSE_RANK() function.")
        DENSE,
        @Label(value = "Ordinal (1, 2, 3, 4...)",
            description = "Unique rank for each row. Uses ROW_NUMBER() function.")
        ORDINAL;
    }

    enum RankDataType {
        @Label(value = "Long", description = "64-bit integer, recommended for large datasets.")
        LONG,
        @Label(value = "Integer", description = "32-bit integer, suitable for small datasets.")
        INTEGER;
    }

    // ── COLUMN CHOICES PROVIDER ──────────────────────────────────────────────

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

    // ── RANK CRITERION (ArrayWidget element) ─────────────────────────────────

    /**
     * A single ranking criterion consisting of a column and a sort order.
     * Used as the element type of the {@code @ArrayWidget} ranking columns array.
     */
    static class RankCriterion implements NodeParameters {
        @Widget(title = "Column", description = "The column to rank by.")
        @ChoicesProvider(SparkColumnChoicesProvider.class)
        String m_column = "";

        @Widget(title = "Order", description = "The sort order for this column.")
        @ValueSwitchWidget
        SortOrder m_order = SortOrder.ASCENDING;

        /** Default constructor. */
        RankCriterion() {}

        /** Constructor with values. */
        RankCriterion(final String column, final SortOrder order) {
            m_column = column;
            m_order = order;
        }
    }

    // ── CUSTOM PERSISTORS ────────────────────────────────────────────────────

    /**
     * Bridges the RankCriterion[] array to/from the parallel String arrays
     * (rankingColumns[], rankingOrders[]) used by SparkRankSettings.
     */
    static final class RankCriteriaPersistor implements NodeParametersPersistor<RankCriterion[]> {

        @Override
        public RankCriterion[] load(final NodeSettingsRO settings) throws InvalidSettingsException {
            if (!settings.containsKey(SparkRankSettings.CFG_RANKING_COLUMNS)) {
                return new RankCriterion[]{new RankCriterion()};
            }
            final String[] columns = settings.getStringArray(SparkRankSettings.CFG_RANKING_COLUMNS);
            final String[] orders = settings.getStringArray(SparkRankSettings.CFG_RANKING_ORDERS, new String[0]);
            final RankCriterion[] criteria = new RankCriterion[columns.length];
            for (int i = 0; i < columns.length; i++) {
                final String orderStr = (i < orders.length) ? orders[i] : "ASCENDING";
                SortOrder order;
                try {
                    order = SortOrder.valueOf(orderStr);
                } catch (final IllegalArgumentException e) {
                    order = SortOrder.ASCENDING;
                }
                criteria[i] = new RankCriterion(columns[i], order);
            }
            return criteria;
        }

        @Override
        public void save(final RankCriterion[] obj, final NodeSettingsWO settings) {
            final RankCriterion[] criteria = (obj != null) ? obj : new RankCriterion[0];
            final String[] columns = new String[criteria.length];
            final String[] orders = new String[criteria.length];
            for (int i = 0; i < criteria.length; i++) {
                columns[i] = criteria[i].m_column != null ? criteria[i].m_column : "";
                orders[i] = criteria[i].m_order != null ? criteria[i].m_order.name() : "ASCENDING";
            }
            settings.addStringArray(SparkRankSettings.CFG_RANKING_COLUMNS, columns);
            settings.addStringArray(SparkRankSettings.CFG_RANKING_ORDERS, orders);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{
                {SparkRankSettings.CFG_RANKING_COLUMNS},
                {SparkRankSettings.CFG_RANKING_ORDERS}
            };
        }
    }

    /**
     * Bridges ColumnFilter to/from the NameFilterConfiguration format used by SparkRankSettings
     * for group columns.
     */
    static final class GroupColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkRankSettings.CFG_GROUP_COLUMNS;

        GroupColumnsPersistor() {
            super(KEY);
        }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return loadColumnFilterWithFallback(settings, KEY);
        }
    }

    /**
     * Loads a ColumnFilter from settings, trying NameFilterConfiguration format first,
     * falling back to SettingsModelFilterString format for backward compatibility.
     */
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

    /**
     * Constructs a ColumnFilter from column names in NameFilterConfiguration format.
     */
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

    /**
     * Bridges RankMode enum to/from the string format used by SparkRankSettings.
     */
    static final class RankModePersistor implements NodeParametersPersistor<RankMode> {
        @Override
        public RankMode load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(SparkRankSettings.CFG_RANK_MODE, "STANDARD");
            try {
                return RankMode.valueOf(val);
            } catch (final IllegalArgumentException e) {
                return RankMode.STANDARD;
            }
        }

        @Override
        public void save(final RankMode obj, final NodeSettingsWO settings) {
            settings.addString(SparkRankSettings.CFG_RANK_MODE,
                (obj != null ? obj : RankMode.STANDARD).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SparkRankSettings.CFG_RANK_MODE}};
        }
    }

    /**
     * Bridges RankDataType enum to/from the string format used by SparkRankSettings.
     */
    static final class RankDataTypePersistor implements NodeParametersPersistor<RankDataType> {
        @Override
        public RankDataType load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(SparkRankSettings.CFG_RANK_DATA_TYPE, "LONG");
            try {
                return RankDataType.valueOf(val);
            } catch (final IllegalArgumentException e) {
                return RankDataType.LONG;
            }
        }

        @Override
        public void save(final RankDataType obj, final NodeSettingsWO settings) {
            settings.addString(SparkRankSettings.CFG_RANK_DATA_TYPE,
                (obj != null ? obj : RankDataType.LONG).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SparkRankSettings.CFG_RANK_DATA_TYPE}};
        }
    }

    // ── FIELDS ───────────────────────────────────────────────────────────────

    // ── Ranking Columns ──────────────────────────────────────────────────────
    @Layout(DialogSections.RankingColumnsSection.class)
    @Widget(title = "Ranking Columns",
        description = "Define the columns and sort orders used to compute the rank. "
            + "If multiple criteria are defined, ranking is first applied by the first criterion; "
            + "additional criteria are only considered in the event of a tie.")
    @ArrayWidget(elementTitle = "Criterion", addButtonText = "Add ranking criterion", showSortButtons = true)
    @Persistor(RankCriteriaPersistor.class)
    RankCriterion[] m_rankingCriteria = new RankCriterion[]{new RankCriterion()};

    // ── Group Columns ────────────────────────────────────────────────────────
    @Layout(DialogSections.GroupColumnsSection.class)
    @Widget(title = "Group Columns",
        description = "Optional columns to partition (group) by. "
            + "Ranks are computed separately within each group. "
            + "If no columns are selected, ranking is applied to the entire dataset.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @Persistor(GroupColumnsPersistor.class)
    ColumnFilter m_groupColumns = new ColumnFilter();

    // ── Options ──────────────────────────────────────────────────────────────
    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Rank mode",
        description = "The ranking method to use for handling tied values.")
    @RadioButtonsWidget
    @Persistor(RankModePersistor.class)
    RankMode m_rankMode = RankMode.STANDARD;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Output column name",
        description = "The name of the column that will contain the computed rank values.")
    @Persist(configKey = SparkRankSettings.CFG_OUTPUT_COL_NAME)
    String m_outputColName = "Rank";

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Rank data type",
        description = "The data type of the rank column. Long is recommended for large datasets.")
    @ValueSwitchWidget
    @Persistor(RankDataTypePersistor.class)
    RankDataType m_rankDataType = RankDataType.LONG;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Sort missing values to end",
        description = "If checked, missing (null) values are always placed at the end regardless of sort order.")
    @Persist(configKey = SparkRankSettings.CFG_MISSING_TO_END)
    boolean m_missingToEnd = true;

    // ── CONSTRUCTORS ─────────────────────────────────────────────────────────

    SparkRankNodeParameters() {}

    // ── HELPERS ──────────────────────────────────────────────────────────────

    /**
     * Extracts the manually selected column names from a ColumnFilter.
     */
    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) {
            return new String[0];
        }
        final org.knime.core.webui.node.dialog.defaultdialog.setting.filter.util.ManualFilter mf =
            filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) {
            return new String[0];
        }
        return mf.m_manuallySelected;
    }
}
