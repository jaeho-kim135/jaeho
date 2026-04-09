package org.knime.bigdata.spark.dx.node.preproc.rounddouble;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DoubleValue;
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
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
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

/**
 * Node parameters (WebUI dialog settings) for the Spark Number Rounder node.
 * Defines column selection, number mode, precision, rounding method, and output mode.
 */
@SuppressWarnings("restriction")
class SparkRoundDoubleNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the numeric columns to round.")
        interface ColumnSelectionSection {}

        @Section(title = "Rounding Settings",
            description = "Configure the rounding mode, precision, and method.")
        @After(ColumnSelectionSection.class)
        interface RoundingSettingsSection {}

        @Section(title = "Output Settings",
            description = "Configure whether to replace or append columns.")
        @After(RoundingSettingsSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum NumberMode {
        @Label("Decimal places") DECIMALS,
        @Label("Significant digits") SIGNIFICANT_DIGITS,
        @Label("Integer") INTEGER;
    }

    enum RoundingStandard {
        @Label("Standard (.5 rounds up)") HALF_AWAY_FROM_ZERO,
        @Label("Other advanced methods") OTHER;
    }

    enum RoundingAdvanced {
        @Label("Round up (away from zero)") AWAY_FROM_ZERO,
        @Label("Round down (towards zero)") TOWARDS_ZERO,
        @Label("Round ceiling (+\u221E direction)") TO_LARGER,
        @Label("Round floor (-\u221E direction)") TO_SMALLER,
        @Label("Half down (.5 towards zero)") HALF_TOWARDS_ZERO,
        @Label("Half even (banker's rounding)") HALF_TO_EVEN;
    }

    enum AppendOrReplace {
        @Label("Replace") REPLACE,
        @Label("Append") APPEND;
    }

    // ── PARAMETER REFERENCES ─────────────────────────────────────────────────

    interface NumberModeRef extends ParameterReference<NumberMode> {}

    interface RoundingStandardRef extends ParameterReference<RoundingStandard> {}

    interface AppendOrReplaceRef extends ParameterReference<AppendOrReplace> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    /** Predicate: NumberMode is DECIMALS or SIGNIFICANT_DIGITS (not INTEGER). */
    static final class IsNotIntegerPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(NumberModeRef.class).isOneOf(NumberMode.DECIMALS, NumberMode.SIGNIFICANT_DIGITS);
        }
    }

    /** Predicate: RoundingStandard is OTHER. */
    static final class IsOtherPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(RoundingStandardRef.class).isOneOf(RoundingStandard.OTHER);
        }
    }

    /** Predicate: AppendOrReplace is APPEND. */
    static final class IsAppendPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.APPEND);
        }
    }

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    /** Provides only numeric columns (DoubleValue compatible) from the input Spark DataFrame. */
    static final class SparkNumericColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream()
                    .filter(cs -> cs.getType().isCompatible(DoubleValue.class))
                    .collect(Collectors.toList()))
                .orElse(Collections.<DataColumnSpec>emptyList());
        }
    }

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter to/from the NameFilterConfiguration format used by SparkRoundDoubleSettings.
     */
    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkRoundDoubleSettings.CFG_COLUMNS;

        ColumnsPersistor() {
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
     * Bridges NumberMode enum to/from string format used by SparkRoundDoubleSettings.
     */
    static final class NumberModePersistor implements NodeParametersPersistor<NumberMode> {
        @Override
        public NumberMode load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(SparkRoundDoubleSettings.CFG_NUMBER_MODE, "DECIMALS");
            try {
                return NumberMode.valueOf(val);
            } catch (final IllegalArgumentException e) {
                return NumberMode.DECIMALS;
            }
        }

        @Override
        public void save(final NumberMode obj, final NodeSettingsWO settings) {
            settings.addString(SparkRoundDoubleSettings.CFG_NUMBER_MODE,
                (obj != null ? obj : NumberMode.DECIMALS).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SparkRoundDoubleSettings.CFG_NUMBER_MODE}};
        }
    }

    /**
     * Bridges RoundingStandard enum to/from string format used by SparkRoundDoubleSettings.
     */
    static final class RoundingStandardPersistor implements NodeParametersPersistor<RoundingStandard> {
        @Override
        public RoundingStandard load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(SparkRoundDoubleSettings.CFG_ROUNDING_STANDARD, "HALF_AWAY_FROM_ZERO");
            try {
                return RoundingStandard.valueOf(val);
            } catch (final IllegalArgumentException e) {
                return RoundingStandard.HALF_AWAY_FROM_ZERO;
            }
        }

        @Override
        public void save(final RoundingStandard obj, final NodeSettingsWO settings) {
            settings.addString(SparkRoundDoubleSettings.CFG_ROUNDING_STANDARD,
                (obj != null ? obj : RoundingStandard.HALF_AWAY_FROM_ZERO).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SparkRoundDoubleSettings.CFG_ROUNDING_STANDARD}};
        }
    }

    /**
     * Bridges RoundingAdvanced enum to/from string format used by SparkRoundDoubleSettings.
     */
    static final class RoundingAdvancedPersistor implements NodeParametersPersistor<RoundingAdvanced> {
        @Override
        public RoundingAdvanced load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(SparkRoundDoubleSettings.CFG_ROUNDING_ADVANCED, "AWAY_FROM_ZERO");
            try {
                return RoundingAdvanced.valueOf(val);
            } catch (final IllegalArgumentException e) {
                return RoundingAdvanced.AWAY_FROM_ZERO;
            }
        }

        @Override
        public void save(final RoundingAdvanced obj, final NodeSettingsWO settings) {
            settings.addString(SparkRoundDoubleSettings.CFG_ROUNDING_ADVANCED,
                (obj != null ? obj : RoundingAdvanced.AWAY_FROM_ZERO).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SparkRoundDoubleSettings.CFG_ROUNDING_ADVANCED}};
        }
    }

    /**
     * Bridges AppendOrReplace enum to/from string format used by SparkRoundDoubleSettings.
     */
    static final class AppendOrReplacePersistor implements NodeParametersPersistor<AppendOrReplace> {
        @Override
        public AppendOrReplace load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(SparkRoundDoubleSettings.CFG_APPEND_OR_REPLACE, "REPLACE");
            try {
                return AppendOrReplace.valueOf(val);
            } catch (final IllegalArgumentException e) {
                return AppendOrReplace.REPLACE;
            }
        }

        @Override
        public void save(final AppendOrReplace obj, final NodeSettingsWO settings) {
            settings.addString(SparkRoundDoubleSettings.CFG_APPEND_OR_REPLACE,
                (obj != null ? obj : AppendOrReplace.REPLACE).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SparkRoundDoubleSettings.CFG_APPEND_OR_REPLACE}};
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Selection ─────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnSelectionSection.class)
    @Widget(title = "Columns to round",
        description = "Select the numeric columns whose values should be rounded.")
    @ColumnFilterWidget(choicesProvider = SparkNumericColumnChoicesProvider.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // ── Rounding Settings ────────────────────────────────────────────────────

    @Layout(DialogSections.RoundingSettingsSection.class)
    @Widget(title = "Number mode",
        description = "Choose the rounding mode: Decimal places, Significant digits, or Integer.")
    @ValueSwitchWidget
    @ValueReference(NumberModeRef.class)
    @Persist(configKey = SparkRoundDoubleSettings.CFG_NUMBER_MODE)
    NumberMode m_numberMode = NumberMode.DECIMALS;

    @Layout(DialogSections.RoundingSettingsSection.class)
    @Widget(title = "Precision",
        description = "The number of decimal places or significant digits to round to.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @Effect(predicate = IsNotIntegerPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkRoundDoubleSettings.CFG_PRECISION)
    int m_precision = 3;

    @Layout(DialogSections.RoundingSettingsSection.class)
    @Widget(title = "Rounding method",
        description = "Choose Standard rounding (.5 rounds up) or select 'Other' for advanced rounding methods.")
    @ValueSwitchWidget
    @ValueReference(RoundingStandardRef.class)
    @Persist(configKey = SparkRoundDoubleSettings.CFG_ROUNDING_STANDARD)
    RoundingStandard m_roundingStandard = RoundingStandard.HALF_AWAY_FROM_ZERO;

    @Layout(DialogSections.RoundingSettingsSection.class)
    @Widget(title = "Advanced rounding method",
        description = "Select the advanced rounding method to apply.")
    @RadioButtonsWidget
    @Effect(predicate = IsOtherPredicate.class, type = EffectType.SHOW)
    @Persistor(RoundingAdvancedPersistor.class)
    RoundingAdvanced m_roundingAdvanced = RoundingAdvanced.AWAY_FROM_ZERO;

    // ── Output Settings ──────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output mode",
        description = "Replace the original columns or append new columns with a suffix.")
    @ValueSwitchWidget
    @ValueReference(AppendOrReplaceRef.class)
    @Persist(configKey = SparkRoundDoubleSettings.CFG_APPEND_OR_REPLACE)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.REPLACE;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Suffix",
        description = "The suffix appended to column names when using Append mode.")
    @TextInputWidget(placeholder = " (Rounded)")
    @Effect(predicate = IsAppendPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkRoundDoubleSettings.CFG_SUFFIX)
    String m_suffix = " (Rounded)";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkRoundDoubleNodeParameters() {
    }

    // ── HELPERS ───────────────────────────────────────────────────────────────

    /**
     * Extracts the manually selected column names from a ColumnFilter.
     */
    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) {
            return new String[0];
        }
        final ManualFilter mf = filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) {
            return new String[0];
        }
        return mf.m_manuallySelected;
    }
}
