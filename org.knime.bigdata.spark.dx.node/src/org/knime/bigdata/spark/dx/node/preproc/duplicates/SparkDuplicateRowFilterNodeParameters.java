package org.knime.bigdata.spark.dx.node.preproc.duplicates;

import java.util.List;

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
import org.knime.node.parameters.updates.util.BooleanReference;

/**
 * Node parameters (WebUI dialog settings) for the Spark Duplicate Row Filter node.
 * Persistors bridge between the WebUI ColumnFilter representation and the
 * SettingsModelFilterString format used by SparkDuplicateRowFilterSettings, ensuring
 * backward compatibility with existing saved workflows.
 */
@SuppressWarnings("restriction")
class SparkDuplicateRowFilterNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Duplicate Detection",
            description = "Select columns used to detect duplicate rows.")
        interface DuplicateDetectionSection {}

        @Section(title = "Duplicate Handling",
            description = "Choose how to handle detected duplicates.")
        @After(DuplicateDetectionSection.class)
        interface DuplicateHandlingSection {}

        @Section(title = "Row Selection",
            description = "Select which row to keep from each duplicate group.")
        @After(DuplicateHandlingSection.class)
        interface RowSelectionSection {}

        @Section(title = "Order Column",
            description = "Column used to determine row ordering within duplicate groups.")
        @After(RowSelectionSection.class)
        interface OrderColumnSection {}

        @Section(title = "Output Columns",
            description = "Configure additional output columns for annotate mode.")
        @After(OrderColumnSection.class)
        interface OutputColumnsSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum DuplicateHandling {
        @Label("Remove duplicates") REMOVE,
        @Label("Keep duplicates (annotate)") KEEP;
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

    interface DupHandlingRef extends ParameterReference<DuplicateHandling> {}
    interface RowSelRef extends ParameterReference<RowSelection> {}
    interface AddStatusRef extends BooleanReference {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    static final class IsRemovePredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(DupHandlingRef.class).isOneOf(DuplicateHandling.REMOVE);
        }
    }

    static final class IsKeepPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(DupHandlingRef.class).isOneOf(DuplicateHandling.KEEP);
        }
    }

    static final class IsFirstOrLastPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(RowSelRef.class).isOneOf(RowSelection.FIRST, RowSelection.LAST);
        }
    }

    static final class AddStatusPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            // Show status column name only when KEEP mode AND addStatusColumn is checked
            return i.getEnum(DupHandlingRef.class).isOneOf(DuplicateHandling.KEEP)
                .and(i.getBoolean(AddStatusRef.class).isTrue());
        }
    }

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    static final class SparkAllColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().toList())
                .orElse(List.of());
        }
    }

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter <-> the settings under key "columns".
     * Extends LegacyColumnFilterPersistor to use the correct config key.
     * Overrides load() to also handle old SettingsModelFilterString format.
     */
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

    /**
     * Loads a ColumnFilter from settings, trying new NameFilterConfiguration format first
     * (filter-type / included_names) and falling back to old SettingsModelFilterString format
     * (InclList / ExclList) for backward compatibility.
     */
    private static ColumnFilter loadColumnFilterWithFallback(final NodeSettingsRO settings,
            final String key) throws InvalidSettingsException {
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey("included_names")) {
                // New format: NameFilterConfiguration / LegacyColumnFilterPersistor
                return LegacyColumnFilterPersistor.load(settings, key);
            }
            // Old format: SettingsModelFilterString (InclList / ExclList)
            final String[] incl = sub.getStringArray("InclList", new String[0]);
            return buildColumnFilterFromNames(incl, key);
        } catch (final InvalidSettingsException e) {
            // Sub-config missing or type mismatch -- return empty filter as safe default
            return new ColumnFilter();
        }
    }

    /**
     * Constructs a ColumnFilter with the given selected column names by building
     * a temporary NodeSettings in NameFilterConfiguration format.
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

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Duplicate Detection ──────────────────────────────────────────────────
    @Layout(DialogSections.DuplicateDetectionSection.class)
    @Widget(title = "Duplicate Detection Columns",
        description = "Columns used to identify duplicate rows. "
            + "Rows with identical values in all selected columns are considered duplicates.")
    @ColumnFilterWidget(choicesProvider = SparkAllColumnChoicesProvider.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // ── Duplicate Handling ───────────────────────────────────────────────────
    @Layout(DialogSections.DuplicateHandlingSection.class)
    @Widget(title = "Duplicate Handling",
        description = "Choose whether to remove duplicate rows or keep all rows with annotations.")
    @ValueSwitchWidget
    @ValueReference(DupHandlingRef.class)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_DUPLICATE_HANDLING)
    DuplicateHandling m_duplicateHandling = DuplicateHandling.REMOVE;

    // ── Row Selection (REMOVE mode only) ─────────────────────────────────────
    @Layout(DialogSections.RowSelectionSection.class)
    @Widget(title = "Row Selection",
        description = "Select which row to keep from each group of duplicates.")
    @RadioButtonsWidget
    @ValueReference(RowSelRef.class)
    @Effect(predicate = IsRemovePredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_ROW_SELECTION)
    RowSelection m_rowSelection = RowSelection.FIRST;

    // ── Order Column (always visible) ────────────────────────────────────────
    @Layout(DialogSections.OrderColumnSection.class)
    @Widget(title = "Order Column",
        description = "Column used to determine row ordering within each duplicate group. "
            + "Required for all modes except 'Remove all duplicates'.")
    @ChoicesProvider(SparkAllColumnChoicesProvider.class)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_ORDER_COLUMN)
    String m_orderColumn = "";

    // ── Order Direction (FIRST/LAST only) ────────────────────────────────────
    @Layout(DialogSections.OrderColumnSection.class)
    @Widget(title = "Order Direction",
        description = "Sort direction for the order column when selecting first or last occurrence.")
    @ValueSwitchWidget
    @Effect(predicate = IsFirstOrLastPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_ORDER_DIRECTION)
    OrderDirection m_orderDirection = OrderDirection.ASC;

    // ── Add Status Column (KEEP mode only) ───────────────────────────────────
    @Layout(DialogSections.OutputColumnsSection.class)
    @Widget(title = "Add status column",
        description = "Add a column indicating whether each row is 'unique', 'chosen', or 'duplicate'.")
    @ValueReference(AddStatusRef.class)
    @Effect(predicate = IsKeepPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_ADD_STATUS_COLUMN)
    boolean m_addStatusColumn = false;

    // ── Status Column Name (when addStatusColumn is true) ────────────────────
    @Layout(DialogSections.OutputColumnsSection.class)
    @Widget(title = "Status column name",
        description = "Name for the status column that will be appended to the output.")
    @Effect(predicate = AddStatusPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDuplicateRowFilterSettings.CFG_STATUS_COLUMN_NAME)
    String m_statusColumnName = "Duplicate Status";

    // ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    SparkDuplicateRowFilterNodeParameters() {}

    // ── HELPERS ───────────────────────────────────────────────────────────────

    /**
     * Extracts the manually selected column names from a ColumnFilter.
     */
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
