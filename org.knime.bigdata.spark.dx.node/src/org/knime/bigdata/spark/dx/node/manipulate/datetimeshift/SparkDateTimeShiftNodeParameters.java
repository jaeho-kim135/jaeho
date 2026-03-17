package org.knime.bigdata.spark.dx.node.manipulate.datetimeshift;

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
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Date&amp;Time Shift node.
 * Controls column selection, shift mode (fixed/column), granularity, and output mode.
 */
@SuppressWarnings("restriction")
class SparkDateTimeShiftNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the date/time columns to shift.")
        interface ColumnSection {}

        @Section(title = "Shift Settings",
            description = "Configure the shift value, mode, and granularity.")
        @After(ColumnSection.class)
        interface ShiftSection {}

        @Section(title = "Output Settings",
            description = "Configure how the result is written to the output.")
        @After(ShiftSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum ShiftMode {
        @Label("Fixed value") FIXED,
        @Label("Column value") COLUMN;
    }

    enum ShiftGranularity {
        @Label("Years") YEAR,
        @Label("Months") MONTH,
        @Label("Weeks") WEEK,
        @Label("Days") DAY,
        @Label("Hours") HOUR,
        @Label("Minutes") MINUTE,
        @Label("Seconds") SECOND,
        @Label("Milliseconds") MILLISECOND;
    }

    enum AppendOrReplace {
        @Label("Replace") REPLACE,
        @Label("Append") APPEND;
    }

    // ── PARAMETER REFERENCES ─────────────────────────────────────────────────

    interface ShiftModeRef extends ParameterReference<ShiftMode> {}

    interface AppendOrReplaceRef extends ParameterReference<AppendOrReplace> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    static final class IsFixedPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(ShiftModeRef.class).isOneOf(ShiftMode.FIXED);
        }
    }

    static final class IsColumnPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(ShiftModeRef.class).isOneOf(ShiftMode.COLUMN);
        }
    }

    static final class IsAppendPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.APPEND);
        }
    }

    // ── COLUMN CHOICES PROVIDERS ──────────────────────────────────────────────

    /**
     * Provides all columns from the Spark input port for the column filter.
     */
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
     * Bridges ColumnFilter to the settings under key "columns".
     */
    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkDateTimeShiftSettings.CFG_COLUMNS;

        ColumnsPersistor() {
            super(KEY);
        }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            try {
                final NodeSettingsRO sub = settings.getNodeSettings(KEY);
                if (sub.containsKey("included_names")) {
                    return LegacyColumnFilterPersistor.load(settings, KEY);
                }
                final String[] incl = sub.getStringArray("InclList", new String[0]);
                final NodeSettings temp = new NodeSettings("_tmp");
                final NodeSettingsWO tsub = temp.addNodeSettings(KEY);
                tsub.addString("filter-type", "STANDARD");
                tsub.addStringArray("included_names", incl);
                tsub.addStringArray("excluded_names", new String[0]);
                tsub.addString("enforce_option", "EnforceInclusion");
                return LegacyColumnFilterPersistor.load(temp, KEY);
            } catch (final InvalidSettingsException e) {
                return new ColumnFilter();
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Selection ────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnSection.class)
    @Widget(title = "Date/Time columns",
        description = "Select the date/time columns to shift.")
    @ColumnFilterWidget(choicesProvider = SparkAllColumnChoicesProvider.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // ── Shift Settings ──────────────────────────────────────────────────────

    @Layout(DialogSections.ShiftSection.class)
    @Widget(title = "Shift mode",
        description = "Fixed value: shift by a constant integer. "
            + "Column value: shift by the integer value in a column (per row).")
    @ValueSwitchWidget
    @ValueReference(ShiftModeRef.class)
    @Persist(configKey = SparkDateTimeShiftSettings.CFG_SHIFT_MODE)
    ShiftMode m_shiftMode = ShiftMode.FIXED;

    @Layout(DialogSections.ShiftSection.class)
    @Widget(title = "Shift value",
        description = "The fixed integer amount to shift. Positive values shift forward, "
            + "negative values shift backward.")
    @NumberInputWidget
    @Effect(predicate = IsFixedPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDateTimeShiftSettings.CFG_SHIFT_VALUE)
    int m_shiftValue = 1;

    @Layout(DialogSections.ShiftSection.class)
    @Widget(title = "Shift column",
        description = "The column containing integer shift values. "
            + "Each row's date/time will be shifted by the value in this column.")
    @ChoicesProvider(SparkAllColumnChoicesProvider.class)
    @Effect(predicate = IsColumnPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDateTimeShiftSettings.CFG_SHIFT_COLUMN)
    String m_shiftColumn = "";

    @Layout(DialogSections.ShiftSection.class)
    @Widget(title = "Granularity",
        description = "The time unit of the shift amount.")
    @RadioButtonsWidget
    @Persist(configKey = SparkDateTimeShiftSettings.CFG_GRANULARITY)
    ShiftGranularity m_granularity = ShiftGranularity.DAY;

    // ── Output Settings ─────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output mode",
        description = "Replace: overwrites the selected columns. "
            + "Append: keeps the originals and adds new columns with a suffix.")
    @ValueSwitchWidget
    @ValueReference(AppendOrReplaceRef.class)
    @Persist(configKey = SparkDateTimeShiftSettings.CFG_APPEND_OR_REPLACE)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.REPLACE;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Suffix",
        description = "Suffix appended to the column name for the new shifted columns.")
    @TextInputWidget(placeholder = "_shifted")
    @Effect(predicate = IsAppendPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDateTimeShiftSettings.CFG_SUFFIX)
    String m_suffix = "_shifted";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkDateTimeShiftNodeParameters() {
    }
}
