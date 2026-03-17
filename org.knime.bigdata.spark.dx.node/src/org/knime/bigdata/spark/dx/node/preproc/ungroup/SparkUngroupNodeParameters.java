package org.knime.bigdata.spark.dx.node.preproc.ungroup;

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
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Node parameters (WebUI dialog settings) for the Spark Ungroup node.
 * Bridges between the WebUI ColumnFilter representation and the
 * SettingsModelFilterString format used by SparkUngroupSettings.
 */
@SuppressWarnings("restriction")
class SparkUngroupNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Target Columns",
            description = "Select the columns to ungroup (explode).")
        interface ColumnsSection {}

        @Section(title = "Explode Settings",
            description = "Configure how columns are exploded.")
        @After(ColumnsSection.class)
        interface ExplodeSection {}

        @Section(title = "Options")
        @After(ExplodeSection.class)
        interface OptionsSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum ExplodeMode {
        @Label(value = "Auto-detect (Array/Map → explode)",
            description = "Automatically detects Array/Map columns and applies explode. "
                + "String columns are split by the delimiter.")
        AUTO,
        @Label(value = "Split string by delimiter",
            description = "Treats all selected columns as strings, splits by delimiter, then explodes.")
        STRING_SPLIT;
    }

    // ── REFERENCE INTERFACES ─────────────────────────────────────────────────

    interface ExplodeModeRef extends ParameterReference<ExplodeMode> {}

    // ── PREDICATE CLASSES ─────────────────────────────────────────────────────

    static final class IsStringSplitPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(ExplodeModeRef.class).isOneOf(ExplodeMode.STRING_SPLIT);
        }
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

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter ↔ the settings under key "columns".
     * Handles both new NameFilterConfiguration format and old SettingsModelFilterString format.
     */
    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkUngroupSettings.CFG_COLUMNS;

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
     * and falling back to old SettingsModelFilterString format for backward compatibility.
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
     * Constructs a ColumnFilter with the given selected column names.
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

    // ── Target Columns ───────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnsSection.class)
    @Widget(title = "Target Columns",
        description = "Columns to ungroup (explode). Each element in an array/map column "
            + "or each token from a delimited string becomes a separate row.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // ── Explode Settings ─────────────────────────────────────────────────────

    @Layout(DialogSections.ExplodeSection.class)
    @Widget(title = "Explode mode",
        description = "How to handle the selected columns for ungrouping.")
    @RadioButtonsWidget
    @ValueReference(ExplodeModeRef.class)
    @Persist(configKey = SparkUngroupSettings.CFG_EXPLODE_MODE)
    ExplodeMode m_explodeMode = ExplodeMode.AUTO;

    @Layout(DialogSections.ExplodeSection.class)
    @Widget(title = "Delimiter",
        description = "The delimiter used to split string values. Only used in 'Split string by delimiter' mode.")
    @TextInputWidget(placeholder = ",")
    @Effect(predicate = IsStringSplitPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkUngroupSettings.CFG_DELIMITER)
    String m_delimiter = ",";

    // ── Options ──────────────────────────────────────────────────────────────

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Remove original column",
        description = "If checked, the original column is removed and replaced by the exploded column. "
            + "If unchecked, the exploded values are added as a new column with '_exploded' suffix.")
    @Persist(configKey = SparkUngroupSettings.CFG_REMOVE_ORIGINAL)
    boolean m_removeOriginal = true;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Skip null rows",
        description = "If checked, rows with null values are excluded from the output (uses explode). "
            + "If unchecked, null values are preserved as null rows (uses explode_outer).")
    @Persist(configKey = SparkUngroupSettings.CFG_SKIP_NULLS)
    boolean m_skipNulls = false;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Skip empty collections",
        description = "If checked, rows with empty strings or empty collections are excluded from the output. "
            + "This is independent of the 'Skip null rows' option.")
    @Persist(configKey = SparkUngroupSettings.CFG_SKIP_EMPTY)
    boolean m_skipEmpty = false;

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkUngroupNodeParameters() {
    }
}
