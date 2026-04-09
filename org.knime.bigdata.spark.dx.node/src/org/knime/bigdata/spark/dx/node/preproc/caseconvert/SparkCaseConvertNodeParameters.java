package org.knime.bigdata.spark.dx.node.preproc.caseconvert;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.StringValue;
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
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.updates.ButtonReference;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.widget.message.TextMessage;

/**
 * Node parameters (WebUI dialog settings) for the Spark Case Converter node.
 * Controls column selection (string columns only) and case conversion mode.
 */
@SuppressWarnings("restriction")
class SparkCaseConvertNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select string columns to convert.")
        interface ColumnSection {}

        @Section(title = "Settings",
            description = "Choose the case conversion mode.")
        @After(ColumnSection.class)
        interface SettingsSection {}

        @Section(title = "Evaluate")
        @After(SettingsSection.class)
        interface EvaluateSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum CaseMode {
        @Label(value = "UPPERCASE", description = "Convert all characters to uppercase.")
        UPPERCASE,
        @Label(value = "lowercase", description = "Convert all characters to lowercase.")
        LOWERCASE,
        @Label(value = "Title Case", description = "Convert first character of each word to uppercase.")
        TITLE_CASE;
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface ColumnsRef extends ParameterReference<ColumnFilter> {}
    interface CaseModeRef extends ParameterReference<CaseMode> {}
    interface EvaluateButtonRef extends ButtonReference {}

    // ── COLUMN CHOICES PROVIDER (String columns only) ─────────────────────────

    /**
     * Provides only String-compatible columns from the Spark input port.
     */
    static final class SparkStringColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream()
                    .filter(cs -> cs.getType().isCompatible(StringValue.class))
                    .collect(Collectors.toList()))
                .orElse(Collections.<DataColumnSpec>emptyList());
        }
    }

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter to the settings under key "columns".
     * Extends LegacyColumnFilterPersistor to use the correct config key.
     * Overrides load() to also handle old SettingsModelFilterString format.
     */
    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkCaseConvertSettings.CFG_COLUMNS;

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

    /**
     * Custom persistor for CaseMode enum.
     */
    static final class CaseModePersistor implements NodeParametersPersistor<CaseMode> {
        private static final String CFG_KEY = SparkCaseConvertSettings.CFG_MODE;

        @Override
        public CaseMode load(final NodeSettingsRO settings) throws InvalidSettingsException {
            String val = settings.getString(CFG_KEY, "UPPERCASE");
            switch (val) {
                case "LOWERCASE":
                    return CaseMode.LOWERCASE;
                case "TITLE_CASE":
                    return CaseMode.TITLE_CASE;
                default:
                    return CaseMode.UPPERCASE;
            }
        }

        @Override
        public void save(final CaseMode obj, final NodeSettingsWO settings) {
            settings.addString(CFG_KEY, (obj != null ? obj : CaseMode.UPPERCASE).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    // ── EVALUATE STATE PROVIDER ───────────────────────────────────────────────

    /**
     * Runs evaluation when the Evaluate button is clicked.
     * Executes the Spark job in validate-only mode and returns a preview.
     */
    static final class EvaluateResultProvider
        implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<ColumnFilter> m_columnsSupplier;
        private Supplier<CaseMode> m_modeSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeOnButtonClick(EvaluateButtonRef.class);
            m_columnsSupplier = initializer.getValueSupplier(ColumnsRef.class);
            m_modeSupplier = initializer.getValueSupplier(CaseModeRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (!portObjOpt.isPresent()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable evaluation.",
                    "", TextMessage.MessageType.INFO));
            }

            String[] cols = getManuallySelected(m_columnsSupplier.get());
            if (cols.length == 0) {
                return Optional.of(new TextMessage.Message(
                    "Select at least one column.",
                    "", TextMessage.MessageType.WARNING));
            }

            SparkDataPortObject sparkPort = (SparkDataPortObject) portObjOpt.get();
            SparkContextID contextID = sparkPort.getContextID();
            String inputObjectId = sparkPort.getData().getID();

            try {
                CaseMode mode = m_modeSupplier.get();
                String modeStr = (mode != null ? mode : CaseMode.UPPERCASE).name();

                SparkCaseConvertJobInput jobInput = new SparkCaseConvertJobInput(
                    inputObjectId, cols, modeStr);

                SparkCaseConvertJobOutput output = SparkContextUtil
                    .<SparkCaseConvertJobInput, SparkCaseConvertJobOutput>getJobRunFactory(
                        contextID, SparkCaseConvertNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(contextID);

                String preview = output.getPreviewData();
                return Optional.of(new TextMessage.Message(
                    "Evaluation succeeded.\n" + (preview != null ? preview : ""),
                    "", TextMessage.MessageType.SUCCESS));
            } catch (Exception e) {
                return Optional.of(new TextMessage.Message(
                    "Evaluation failed: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()),
                    "", TextMessage.MessageType.ERROR));
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnSection.class)
    @Widget(title = "String Columns",
        description = "Select the string columns whose case to convert.")
    @ColumnFilterWidget(choicesProvider = SparkStringColumnChoicesProvider.class)
    @ValueReference(ColumnsRef.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "Case Mode",
        description = "Choose the target case for the selected columns.")
    @RadioButtonsWidget(horizontal = true)
    @ValueReference(CaseModeRef.class)
    @Persist(configKey = SparkCaseConvertSettings.CFG_MODE)
    CaseMode m_mode = CaseMode.UPPERCASE;

    @Layout(DialogSections.EvaluateSection.class)
    @Widget(title = "Evaluate",
        description = "Preview the result of case conversion.")
    @SimpleButtonWidget(ref = EvaluateButtonRef.class, icon = Icon.RELOAD)
    Void m_evaluateButton;

    @Layout(DialogSections.EvaluateSection.class)
    @TextMessage(EvaluateResultProvider.class)
    Void m_evaluateResult;

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkCaseConvertNodeParameters() {
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
        ManualFilter mf = filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) {
            return new String[0];
        }
        return mf.m_manuallySelected;
    }
}
