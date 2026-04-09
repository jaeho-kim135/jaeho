package org.knime.bigdata.spark.dx.node.preproc.colcombine;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.Icon;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.SimpleButtonWidget;
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
import org.knime.node.parameters.updates.ButtonReference;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.widget.message.TextMessage;

/**
 * Node parameters (WebUI dialog settings) for the Spark Column Combiner node.
 */
@SuppressWarnings("restriction")
class SparkColumnCombinerNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Columns",
            description = "Select the columns to combine into a single string column.")
        interface ColumnsSection {}

        @Section(title = "Options")
        @After(ColumnsSection.class)
        interface OptionsSection {}

        @Section(title = "Quoting")
        @After(OptionsSection.class)
        interface QuotingSection {}

        @Section(title = "Validation")
        @After(QuotingSection.class)
        interface ValidationSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum MissingHandling {
        @Label(value = "Skip (omit from result)",
            description = "Missing values are skipped during concatenation (CONCAT_WS default behavior).")
        SKIP,
        @Label(value = "Include as empty string",
            description = "Missing values are replaced with empty strings before concatenation.")
        AS_EMPTY;
    }

    enum QuoteMode {
        @Label(value = "No quoting",
            description = "No special handling of delimiter occurrences within cell values.")
        NONE,
        @Label(value = "Quote cells containing delimiter",
            description = "Cells containing the delimiter are wrapped in the quote character.")
        QUOTE,
        @Label(value = "Replace delimiter in cells",
            description = "Occurrences of the delimiter within cells are replaced with a substitute string.")
        REPLACE_IN_CELL;
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface ColumnsRef extends ParameterReference<ColumnFilter> {}
    interface OutputColNameRef extends ParameterReference<String> {}
    interface QuoteModeRef extends ParameterReference<QuoteMode> {}

    /** Button reference for the Check / Run Validation button. */
    interface CheckButtonRef extends ButtonReference {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    /** Shows m_quoteChar when quote mode is QUOTE. */
    static final class IsQuotePredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(QuoteModeRef.class).isOneOf(QuoteMode.QUOTE);
        }
    }

    /** Shows m_replacementDelimiter when quote mode is REPLACE_IN_CELL. */
    static final class IsReplaceInCellPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(QuoteModeRef.class).isOneOf(QuoteMode.REPLACE_IN_CELL);
        }
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

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter to the settings under key "columns".
     */
    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkColumnCombinerSettings.CFG_COLUMNS;

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

    // ── VALIDATION STATE PROVIDER ─────────────────────────────────────────────

    /**
     * Runs validation when the Check button is clicked.
     */
    static final class ValidationProvider
        implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<ColumnFilter> m_columnsSupplier;
        private Supplier<String> m_outputColNameSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeOnButtonClick(CheckButtonRef.class);
            m_columnsSupplier = initializer.getValueSupplier(ColumnsRef.class);
            m_outputColNameSupplier = initializer.getValueSupplier(OutputColNameRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (!portObjOpt.isPresent()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable validation.",
                    "", TextMessage.MessageType.INFO));
            }

            String[] cols = getManuallySelected(m_columnsSupplier.get());
            if (cols.length < 2) {
                return Optional.of(new TextMessage.Message(
                    "Select at least 2 columns to combine.",
                    "", TextMessage.MessageType.WARNING));
            }

            String outputColName = m_outputColNameSupplier.get();
            if (outputColName == null || outputColName.trim().isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Output column name must not be empty.",
                    "", TextMessage.MessageType.WARNING));
            }

            return Optional.of(new TextMessage.Message(
                String.format("Validation succeeded. Combining %d columns.", cols.length),
                "", TextMessage.MessageType.SUCCESS));
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Columns ──────────────────────────────────────────────────────────────
    @Layout(DialogSections.ColumnsSection.class)
    @Widget(title = "Columns to Combine",
        description = "Select the columns whose values will be concatenated into a single string column.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @ValueReference(ColumnsRef.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // ── Options ───────────────────────────────────────────────────────────────
    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Delimiter",
        description = "The string used to separate combined values.")
    @Persist(configKey = SparkColumnCombinerSettings.CFG_DELIMITER)
    String m_delimiter = ",";

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Output column name",
        description = "Name of the new column containing the combined string values.")
    @Persist(configKey = SparkColumnCombinerSettings.CFG_OUTPUT_COL_NAME)
    @ValueReference(OutputColNameRef.class)
    String m_outputColName = "Combined";

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Remove input columns",
        description = "If checked, the original input columns are removed from the output.")
    @Persist(configKey = SparkColumnCombinerSettings.CFG_REMOVE_INPUT_COLS)
    boolean m_removeInputCols = false;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Missing value handling",
        description = "How to handle missing (null) values during concatenation.")
    @ValueSwitchWidget
    @Persist(configKey = SparkColumnCombinerSettings.CFG_HANDLE_MISSING)
    MissingHandling m_handleMissing = MissingHandling.SKIP;

    // ── Quoting ───────────────────────────────────────────────────────────────
    @Layout(DialogSections.QuotingSection.class)
    @Widget(title = "Quote mode",
        description = "How to handle delimiter characters that appear within cell values.")
    @RadioButtonsWidget
    @Persist(configKey = SparkColumnCombinerSettings.CFG_QUOTE_MODE)
    @ValueReference(QuoteModeRef.class)
    QuoteMode m_quoteMode = QuoteMode.NONE;

    @Layout(DialogSections.QuotingSection.class)
    @Widget(title = "Quote character",
        description = "The character used to quote cells containing the delimiter.")
    @Effect(predicate = IsQuotePredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkColumnCombinerSettings.CFG_QUOTE_CHAR)
    String m_quoteChar = "\"";

    @Layout(DialogSections.QuotingSection.class)
    @Widget(title = "Replacement for delimiter in cells",
        description = "The string used to replace delimiter occurrences within cell values.")
    @Effect(predicate = IsReplaceInCellPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkColumnCombinerSettings.CFG_REPLACEMENT_DELIMITER)
    String m_replacementDelimiter = "";

    // ── Validation ────────────────────────────────────────────────────────────
    @Layout(DialogSections.ValidationSection.class)
    @Widget(title = "Run Validation",
        description = "Validate the current configuration. Requires the upstream node to be executed.")
    @SimpleButtonWidget(ref = CheckButtonRef.class, icon = Icon.RELOAD)
    Void m_checkButton;

    @Layout(DialogSections.ValidationSection.class)
    @TextMessage(ValidationProvider.class)
    Void m_validationDisplay;

    // ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    SparkColumnCombinerNodeParameters() {}

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
