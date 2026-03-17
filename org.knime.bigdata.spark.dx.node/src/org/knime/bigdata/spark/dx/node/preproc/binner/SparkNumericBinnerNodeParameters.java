package org.knime.bigdata.spark.dx.node.preproc.binner;

import java.util.List;

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
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.WidgetGroup;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Numeric Binner node.
 * Provides a modern WebUI dialog with column filter for numeric columns,
 * binning mode selection (equal width, equal frequency, custom),
 * and bin definition controls.
 */
@SuppressWarnings("restriction")
class SparkNumericBinnerNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the numeric columns to bin.")
        interface ColumnSelectionSection {}

        @Section(title = "Binning Mode",
            description = "Choose how bins are determined.")
        @After(ColumnSelectionSection.class)
        interface BinningModeSection {}

        @Section(title = "Bin Definitions",
            description = "Define custom bin ranges.")
        @After(BinningModeSection.class)
        interface BinDefinitionSection {}

        @Section(title = "Output Options",
            description = "Configure output column behavior.")
        @After(BinDefinitionSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum BinningMode {
        @Label(value = "Equal width (auto)", description = "Automatically divide the range into equal-width bins.")
        EQUAL_WIDTH,
        @Label(value = "Equal frequency (auto)", description = "Automatically divide data into bins with approximately equal row counts.")
        EQUAL_FREQUENCY,
        @Label(value = "Custom ranges", description = "Manually define bin boundaries.")
        CUSTOM;
    }

    enum BinNaming {
        @Label(value = "Numbered (Bin 1, Bin 2, ...)", description = "Bins are named with sequential numbers.")
        NUMBERED,
        @Label(value = "Borders ([0.0, 10.0))", description = "Bins are named with their boundary values.")
        BORDERS,
        @Label(value = "Midpoints (5.0, 15.0, ...)", description = "Bins are named with their midpoint values.")
        MIDPOINTS;
    }

    enum AppendOrReplace {
        @Label(value = "Replace", description = "Replace the original column with the binned column.")
        REPLACE,
        @Label(value = "Append", description = "Append a new binned column next to the original.")
        APPEND;
    }

    // ── PARAMETER REFERENCES & EFFECT PREDICATES ───────────────────────────────

    interface BinningModeRef extends ParameterReference<BinningMode> {}

    /** Predicate: show when EQUAL_WIDTH or EQUAL_FREQUENCY. */
    static final class IsAutoModePredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(BinningModeRef.class).isOneOf(BinningMode.EQUAL_WIDTH, BinningMode.EQUAL_FREQUENCY);
        }
    }

    /** Predicate: show when CUSTOM. */
    static final class IsCustomPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(BinningModeRef.class).isOneOf(BinningMode.CUSTOM);
        }
    }

    interface AppendOrReplaceRef extends ParameterReference<AppendOrReplace> {}

    /** Predicate: show suffix field when APPEND. */
    static final class IsAppendPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.APPEND);
        }
    }

    // ── COLUMN CHOICES PROVIDER ─────────────────────────────────────────────

    /** Provides only numeric (DoubleValue-compatible) columns. */
    static final class NumericColumnChoicesProvider implements ColumnChoicesProvider {
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

    // ── BIN DEFINITION (for @ArrayWidget) ────────────────────────────────────

    static class BinDefinition implements WidgetGroup {
        @Widget(title = "Bin Name", description = "Name for this bin category.")
        @TextInputWidget(placeholder = "Bin 1")
        @Persist(configKey = "binName")
        String m_binName = "";

        @Widget(title = "Left Boundary", description = "Left (lower) boundary of the bin range.")
        @NumberInputWidget
        @Persist(configKey = "leftBound")
        double m_leftBound = 0.0;

        @Widget(title = "Left Inclusive", description = "Whether the left boundary is inclusive (&gt;=).")
        @Persist(configKey = "leftInclusive")
        boolean m_leftInclusive = true;

        @Widget(title = "Right Boundary", description = "Right (upper) boundary of the bin range.")
        @NumberInputWidget
        @Persist(configKey = "rightBound")
        double m_rightBound = 10.0;

        @Widget(title = "Right Inclusive", description = "Whether the right boundary is inclusive (&lt;=).")
        @Persist(configKey = "rightInclusive")
        boolean m_rightInclusive = false;

        BinDefinition() {
        }
    }

    // ── CUSTOM PERSISTORS ────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter (WebUI) to the settings under key "columns".
     */
    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkNumericBinnerSettings.CFG_COLUMNS;

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
     * and falling back to old SettingsModelFilterString format.
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
     * Bridges BinningMode enum to the string format used by SparkNumericBinnerSettings.
     */
    static final class BinningModePersistor implements NodeParametersPersistor<BinningMode> {
        private static final String CFG_KEY = SparkNumericBinnerSettings.CFG_BINNING_MODE;

        @Override
        public BinningMode load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(CFG_KEY, SparkNumericBinnerSettings.MODE_CUSTOM);
            return switch (val) {
                case SparkNumericBinnerSettings.MODE_EQUAL_WIDTH -> BinningMode.EQUAL_WIDTH;
                case SparkNumericBinnerSettings.MODE_EQUAL_FREQUENCY -> BinningMode.EQUAL_FREQUENCY;
                default -> BinningMode.CUSTOM;
            };
        }

        @Override
        public void save(final BinningMode obj, final NodeSettingsWO settings) {
            final String val = switch (obj != null ? obj : BinningMode.CUSTOM) {
                case EQUAL_WIDTH -> SparkNumericBinnerSettings.MODE_EQUAL_WIDTH;
                case EQUAL_FREQUENCY -> SparkNumericBinnerSettings.MODE_EQUAL_FREQUENCY;
                case CUSTOM -> SparkNumericBinnerSettings.MODE_CUSTOM;
            };
            settings.addString(CFG_KEY, val);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    /**
     * Bridges BinNaming enum to string format.
     */
    static final class BinNamingPersistor implements NodeParametersPersistor<BinNaming> {
        private static final String CFG_KEY = SparkNumericBinnerSettings.CFG_BIN_NAMING;

        @Override
        public BinNaming load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(CFG_KEY, SparkNumericBinnerSettings.NAMING_BORDERS);
            return switch (val) {
                case SparkNumericBinnerSettings.NAMING_NUMBERED -> BinNaming.NUMBERED;
                case SparkNumericBinnerSettings.NAMING_MIDPOINTS -> BinNaming.MIDPOINTS;
                default -> BinNaming.BORDERS;
            };
        }

        @Override
        public void save(final BinNaming obj, final NodeSettingsWO settings) {
            final String val = switch (obj != null ? obj : BinNaming.BORDERS) {
                case NUMBERED -> SparkNumericBinnerSettings.NAMING_NUMBERED;
                case MIDPOINTS -> SparkNumericBinnerSettings.NAMING_MIDPOINTS;
                case BORDERS -> SparkNumericBinnerSettings.NAMING_BORDERS;
            };
            settings.addString(CFG_KEY, val);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    /**
     * Bridges AppendOrReplace enum to string format.
     */
    static final class AppendOrReplacePersistor implements NodeParametersPersistor<AppendOrReplace> {
        private static final String CFG_KEY = SparkNumericBinnerSettings.CFG_APPEND_OR_REPLACE;

        @Override
        public AppendOrReplace load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(CFG_KEY, SparkNumericBinnerSettings.OUTPUT_REPLACE);
            return switch (val) {
                case SparkNumericBinnerSettings.OUTPUT_APPEND -> AppendOrReplace.APPEND;
                default -> AppendOrReplace.REPLACE;
            };
        }

        @Override
        public void save(final AppendOrReplace obj, final NodeSettingsWO settings) {
            final String val = switch (obj != null ? obj : AppendOrReplace.REPLACE) {
                case APPEND -> SparkNumericBinnerSettings.OUTPUT_APPEND;
                case REPLACE -> SparkNumericBinnerSettings.OUTPUT_REPLACE;
            };
            settings.addString(CFG_KEY, val);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    /**
     * Persists BinDefinition[] as parallel string arrays in settings,
     * bridging between the WebUI array representation and the flat settings format.
     */
    static final class BinDefinitionArrayPersistor implements NodeParametersPersistor<BinDefinition[]> {
        @Override
        public BinDefinition[] load(final NodeSettingsRO settings) throws InvalidSettingsException {
            if (!settings.containsKey(SparkNumericBinnerSettings.CFG_BIN_NAMES)) {
                return new BinDefinition[]{new BinDefinition()};
            }
            final String[] names = settings.getStringArray(SparkNumericBinnerSettings.CFG_BIN_NAMES);
            final String[] lefts = settings.getStringArray(
                SparkNumericBinnerSettings.CFG_BIN_LEFT_BOUNDS, new String[0]);
            final String[] leftIncl = settings.getStringArray(
                SparkNumericBinnerSettings.CFG_BIN_LEFT_INCLUSIVE, new String[0]);
            final String[] rights = settings.getStringArray(
                SparkNumericBinnerSettings.CFG_BIN_RIGHT_BOUNDS, new String[0]);
            final String[] rightIncl = settings.getStringArray(
                SparkNumericBinnerSettings.CFG_BIN_RIGHT_INCLUSIVE, new String[0]);

            if (names.length == 0) {
                return new BinDefinition[]{new BinDefinition()};
            }

            final BinDefinition[] bins = new BinDefinition[names.length];
            for (int i = 0; i < names.length; i++) {
                bins[i] = new BinDefinition();
                bins[i].m_binName = names[i];
                bins[i].m_leftBound = i < lefts.length ? parseDouble(lefts[i], 0.0) : 0.0;
                bins[i].m_leftInclusive = i < leftIncl.length ? Boolean.parseBoolean(leftIncl[i]) : true;
                bins[i].m_rightBound = i < rights.length ? parseDouble(rights[i], 0.0) : 0.0;
                bins[i].m_rightInclusive = i < rightIncl.length ? Boolean.parseBoolean(rightIncl[i]) : false;
            }
            return bins;
        }

        @Override
        public void save(final BinDefinition[] bins, final NodeSettingsWO settings) {
            if (bins == null || bins.length == 0) {
                settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_NAMES, new String[0]);
                settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_LEFT_BOUNDS, new String[0]);
                settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_LEFT_INCLUSIVE, new String[0]);
                settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_RIGHT_BOUNDS, new String[0]);
                settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_RIGHT_INCLUSIVE, new String[0]);
                return;
            }
            final String[] names = new String[bins.length];
            final String[] lefts = new String[bins.length];
            final String[] leftIncl = new String[bins.length];
            final String[] rights = new String[bins.length];
            final String[] rightIncl = new String[bins.length];
            for (int i = 0; i < bins.length; i++) {
                names[i] = bins[i].m_binName != null ? bins[i].m_binName : "";
                lefts[i] = String.valueOf(bins[i].m_leftBound);
                leftIncl[i] = String.valueOf(bins[i].m_leftInclusive);
                rights[i] = String.valueOf(bins[i].m_rightBound);
                rightIncl[i] = String.valueOf(bins[i].m_rightInclusive);
            }
            settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_NAMES, names);
            settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_LEFT_BOUNDS, lefts);
            settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_LEFT_INCLUSIVE, leftIncl);
            settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_RIGHT_BOUNDS, rights);
            settings.addStringArray(SparkNumericBinnerSettings.CFG_BIN_RIGHT_INCLUSIVE, rightIncl);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{
                {SparkNumericBinnerSettings.CFG_BIN_NAMES},
                {SparkNumericBinnerSettings.CFG_BIN_LEFT_BOUNDS},
                {SparkNumericBinnerSettings.CFG_BIN_LEFT_INCLUSIVE},
                {SparkNumericBinnerSettings.CFG_BIN_RIGHT_BOUNDS},
                {SparkNumericBinnerSettings.CFG_BIN_RIGHT_INCLUSIVE}
            };
        }

        private static double parseDouble(final String s, final double defaultVal) {
            try {
                return Double.parseDouble(s);
            } catch (final NumberFormatException e) {
                return defaultVal;
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Selection ─────────────────────────────────────────────────────
    @Layout(DialogSections.ColumnSelectionSection.class)
    @Widget(title = "Numeric Columns",
        description = "Select the numeric columns whose values should be binned into categories.")
    @ColumnFilterWidget(choicesProvider = NumericColumnChoicesProvider.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // ── Binning Mode ─────────────────────────────────────────────────────────
    @Layout(DialogSections.BinningModeSection.class)
    @Widget(title = "Binning Mode",
        description = "Choose how to determine the bin boundaries."
            + " Equal width divides the data range into equal-width intervals."
            + " Equal frequency creates bins with approximately equal row counts."
            + " Custom allows manual bin definition.")
    @RadioButtonsWidget
    @ValueReference(BinningModeRef.class)
    @Persist(configKey = SparkNumericBinnerSettings.CFG_BINNING_MODE)
    BinningMode m_binningMode = BinningMode.CUSTOM;

    @Layout(DialogSections.BinningModeSection.class)
    @Widget(title = "Number of Bins",
        description = "Number of bins to create. Only applicable for equal width and equal frequency modes.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @Effect(predicate = IsAutoModePredicate.class, type = EffectType.SHOW)
    @Persist(configKey = "numberOfBins")
    int m_numberOfBins = 5;

    @Layout(DialogSections.BinningModeSection.class)
    @Widget(title = "Bin Naming",
        description = "Choose how auto-generated bins are named."
            + " Numbered: Bin 1, Bin 2, ..."
            + " Borders: [0.0, 10.0)"
            + " Midpoints: 5.0, 15.0, ...")
    @ValueSwitchWidget
    @Effect(predicate = IsAutoModePredicate.class, type = EffectType.SHOW)
    @Persistor(BinNamingPersistor.class)
    BinNaming m_binNaming = BinNaming.BORDERS;

    // ── Bin Definitions (Custom mode) ────────────────────────────────────────
    @Layout(DialogSections.BinDefinitionSection.class)
    @Widget(title = "Bin Definitions",
        description = "Define custom bin ranges. Each bin has a name, left/right boundaries, and "
            + "inclusive/exclusive flags. Use left inclusive and right exclusive for typical binning "
            + "(e.g., [0, 10) means 0 &lt;= x &lt; 10).")
    @ArrayWidget(addButtonText = "Add Bin", showSortButtons = true)
    @Effect(predicate = IsCustomPredicate.class, type = EffectType.SHOW)
    @Persistor(BinDefinitionArrayPersistor.class)
    BinDefinition[] m_bins = new BinDefinition[]{new BinDefinition()};

    // ── Output Options ───────────────────────────────────────────────────────
    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output Mode",
        description = "Choose whether to replace the original numeric column or append a new column.")
    @ValueSwitchWidget
    @ValueReference(AppendOrReplaceRef.class)
    @Persist(configKey = SparkNumericBinnerSettings.CFG_APPEND_OR_REPLACE)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.REPLACE;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Suffix",
        description = "Suffix appended to the original column name for the binned column.")
    @TextInputWidget(placeholder = "_binned")
    @Effect(predicate = IsAppendPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = "suffix")
    String m_suffix = "_binned";

    // ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    SparkNumericBinnerNodeParameters() {
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
