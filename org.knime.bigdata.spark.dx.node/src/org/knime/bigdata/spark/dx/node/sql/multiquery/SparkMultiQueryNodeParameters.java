package org.knime.bigdata.spark.dx.node.sql.multiquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersInputImpl;
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
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.FlowVariableChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.widget.message.TextMessage;

/**
 * Node parameters (WebUI dialog settings) for the Spark Multi Query node.
 */
@SuppressWarnings("restriction")
class SparkMultiQueryNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the target columns to apply the SQL expression to.")
        interface ColumnSelectionSection {}

        @Section(title = "SQL Expression",
            description = "Select a template or enter a custom SQL expression.")
        @After(ColumnSelectionSection.class)
        interface ExpressionSection {}

        @Section(title = "Output Options")
        @After(ExpressionSection.class)
        interface OptionsSection {}

        @Section(title = "SQL Preview",
            description = "Live preview of the SELECT statement that will be generated.")
        @After(OptionsSection.class)
        interface PreviewSection {}

        @Section(title = "Validation",
            description = "Run a test query against the upstream Spark data.")
        @After(PreviewSection.class)
        interface ValidationSection {}
    }

    // ── EXPRESSION TEMPLATE ENUM ──────────────────────────────────────────────

    enum ExpressionTemplate {
        @Label(value = "(Custom)",                       description = "Enter a custom SQL expression.")
        CUSTOM,
        @Label(value = "Cast to String",                 description = "string($columnS)")
        CAST_STRING,
        @Label(value = "Cast to Integer",                description = "CAST($columnS AS INT)")
        CAST_INT,
        @Label(value = "Cast to Double",                 description = "CAST($columnS AS DOUBLE)")
        CAST_DOUBLE,
        @Label(value = "Uppercase",                      description = "UPPER($columnS)")
        UPPERCASE,
        @Label(value = "Lowercase",                      description = "LOWER($columnS)")
        LOWERCASE,
        @Label(value = "Trim",                           description = "TRIM($columnS)")
        TRIM,
        @Label(value = "Replace NULL with 0",            description = "COALESCE($columnS, 0)")
        NULL_TO_ZERO,
        @Label(value = "Replace NULL with empty string", description = "COALESCE($columnS, '')")
        NULL_TO_EMPTY,
        @Label(value = "Parse Date (yyyyMMdd)",          description = "TO_DATE(string($columnS), 'yyyyMMdd')")
        PARSE_DATE,
        @Label(value = "Regex Replace (non-digits)",     description = "REGEXP_REPLACE($columnS, '[^0-9]', '')")
        REGEX_NONDIGITS,
        @Label(value = "Round to 2 decimals",            description = "ROUND($columnS, 2)")
        ROUND_2;

        String getSql() {
            final String ph = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;
            return switch (this) {
                case CAST_STRING     -> "string(" + ph + ")";
                case CAST_INT        -> "CAST(" + ph + " AS INT)";
                case CAST_DOUBLE     -> "CAST(" + ph + " AS DOUBLE)";
                case UPPERCASE       -> "UPPER(" + ph + ")";
                case LOWERCASE       -> "LOWER(" + ph + ")";
                case TRIM            -> "TRIM(" + ph + ")";
                case NULL_TO_ZERO    -> "COALESCE(" + ph + ", 0)";
                case NULL_TO_EMPTY   -> "COALESCE(" + ph + ", '')";
                case PARSE_DATE      -> "TO_DATE(string(" + ph + "), 'yyyyMMdd')";
                case REGEX_NONDIGITS -> "REGEXP_REPLACE(" + ph + ", '[^0-9]', '')";
                case ROUND_2         -> "ROUND(" + ph + ", 2)";
                case CUSTOM          -> "";
            };
        }
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface TargetColumnsRef      extends ParameterReference<ColumnFilter> {}
    interface TemplateRef           extends ParameterReference<ExpressionTemplate> {}
    interface SqlExpressionRef      extends ParameterReference<String> {}
    interface KeepOriginalRef       extends ParameterReference<Boolean> {}
    interface OutputPatternRef      extends ParameterReference<String> {}
    interface FlowVarSelectorRef    extends ParameterReference<String> {}

    interface CheckButtonRef        extends ButtonReference {}
    interface InsertFlowVarButtonRef extends ButtonReference {}

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

    /** Provides all available flow variables for the dropdown. */
    static final class AllFlowVarsProvider implements FlowVariableChoicesProvider {
        @Override
        public List<FlowVariable> flowVariableChoices(final NodeParametersInput context) {
            return ((NodeParametersInputImpl) context)
                .getAvailableInputFlowVariables(
                    VariableTypeRegistry.getInstance().getAllTypes())
                .values().stream().toList();
        }
    }

    // ── PERSISTORS ────────────────────────────────────────────────────────────

    static final class TargetColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkMultiQuerySettings.CFG_TARGET_COLUMNS;

        TargetColumnsPersistor() { super(KEY); }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return loadColumnFilterWithFallback(settings, KEY);
        }
    }

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

    /** Ephemeral persistor for the template dropdown — always resets to CUSTOM on dialog open. */
    static final class EphemeralTemplatePersistor implements NodeParametersPersistor<ExpressionTemplate> {
        @Override public ExpressionTemplate load(final NodeSettingsRO s) { return ExpressionTemplate.CUSTOM; }
        @Override public void save(final ExpressionTemplate o, final NodeSettingsWO s) {}
        @Override public String[][] getConfigPaths() { return new String[0][]; }
    }

    /** Ephemeral persistor for the flow variable selector — always resets to empty on dialog open. */
    static final class EphemeralStringPersistor implements NodeParametersPersistor<String> {
        @Override public String load(final NodeSettingsRO s) { return ""; }
        @Override public void save(final String o, final NodeSettingsWO s) {}
        @Override public String[][] getConfigPaths() { return new String[0][]; }
    }

    // ── STATE PROVIDERS ───────────────────────────────────────────────────────

    /**
     * Updates the SQL expression field:
     * - When a non-CUSTOM template is selected → replaces SQL with template SQL.
     * - When the Insert button is clicked AND template is (Custom) → appends $$varName.
     *
     * Note: if template is non-CUSTOM, it takes priority over the Insert button.
     * Switch to (Custom) before inserting flow variables.
     */
    static final class SqlExpressionValueProvider implements StateProvider<String> {

        private Supplier<ExpressionTemplate> m_templateSupplier;
        private Supplier<String>             m_flowVarSupplier;
        private Supplier<String>             m_currentSqlSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_templateSupplier   = initializer.computeFromValueSupplier(TemplateRef.class);
            initializer.computeOnButtonClick(InsertFlowVarButtonRef.class);
            m_flowVarSupplier    = initializer.getValueSupplier(FlowVarSelectorRef.class);
            m_currentSqlSupplier = initializer.getValueSupplier(SqlExpressionRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput context) {
            final ExpressionTemplate t = m_templateSupplier.get();
            final String flowVar = m_flowVarSupplier.get();
            String currentSql = m_currentSqlSupplier.get();
            if (currentSql == null) currentSql = "";

            // Non-CUSTOM template always takes priority
            if (t != null && t != ExpressionTemplate.CUSTOM) {
                return t.getSql();
            }

            // CUSTOM + non-empty flow variable: Insert button was clicked
            if (flowVar != null && !flowVar.isBlank()) {
                return currentSql + "$$" + flowVar.trim();
            }

            return currentSql;
        }
    }

    /**
     * Live SQL preview — updates whenever columns, expression, or options change.
     */
    static final class SqlPreviewProvider implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<ColumnFilter> m_colSupplier;
        private Supplier<String>       m_sqlSupplier;
        private Supplier<Boolean>      m_keepOrigSupplier;
        private Supplier<String>       m_patternSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_colSupplier      = initializer.computeFromValueSupplier(TargetColumnsRef.class);
            m_sqlSupplier      = initializer.computeFromValueSupplier(SqlExpressionRef.class);
            m_keepOrigSupplier = initializer.computeFromValueSupplier(KeepOriginalRef.class);
            m_patternSupplier  = initializer.computeFromValueSupplier(OutputPatternRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            final String[] cols = getManuallySelected(m_colSupplier.get());
            final String expr = m_sqlSupplier.get();
            final boolean keepOrig = Boolean.TRUE.equals(m_keepOrigSupplier.get());
            final String pattern = m_patternSupplier.get();
            final String ph = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;

            if (cols.length == 0) {
                return Optional.of(new TextMessage.Message(
                    "Select target columns in the 'Column Selection' section.",
                    "", TextMessage.MessageType.INFO));
            }
            if (expr == null || expr.isBlank()) {
                return Optional.of(new TextMessage.Message(
                    "Enter a SQL expression.", "", TextMessage.MessageType.INFO));
            }
            if (!expr.contains(ph)) {
                return Optional.of(new TextMessage.Message(
                    "SQL expression must contain '" + ph + "' as placeholder.",
                    "", TextMessage.MessageType.WARNING));
            }

            // Collect all existing column names for dedup (preview uses target cols as proxy)
            final java.util.Set<String> usedNames = new java.util.LinkedHashSet<>();
            for (final String c : cols) {
                usedNames.add(c);
            }

            final StringBuilder sb = new StringBuilder("SELECT ");
            for (int i = 0; i < cols.length; i++) {
                if (i > 0) sb.append(", ");
                final String col = cols[i];
                if (keepOrig) sb.append("`").append(col).append("`, ");
                final String resolved = expr.replace(ph, "`" + col + "`");
                String alias = (pattern != null && !pattern.isBlank())
                    ? pattern.replace(ph, col) : col;
                // Auto-dedup: if keepOrig and alias == original col name, append _1, _2, ...
                if (keepOrig && usedNames.contains(alias)) {
                    int suffix = 1;
                    while (usedNames.contains(alias + "_" + suffix)) {
                        suffix++;
                    }
                    alias = alias + "_" + suffix;
                }
                usedNames.add(alias);
                sb.append(resolved).append(" AS `").append(alias).append("`");
            }
            if (cols.length < 3) sb.append(", ...");
            sb.append(" FROM input");

            return Optional.of(new TextMessage.Message(
                sb.toString(), "", TextMessage.MessageType.INFO));
        }
    }

    /**
     * Runs the Spark validation job on button click.
     * Tests all columns at once; on failure re-tests per-column to identify which failed.
     */
    static final class ValidationProvider implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<ColumnFilter> m_colSupplier;
        private Supplier<String>       m_sqlSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeOnButtonClick(CheckButtonRef.class);
            m_colSupplier = initializer.getValueSupplier(TargetColumnsRef.class);
            m_sqlSupplier = initializer.getValueSupplier(SqlExpressionRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            final Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (portObjOpt.isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable validation.",
                    "", TextMessage.MessageType.INFO));
            }

            final String[] cols = getManuallySelected(m_colSupplier.get());
            if (cols.length == 0) {
                return Optional.of(new TextMessage.Message(
                    "Select at least one target column to run validation.",
                    "", TextMessage.MessageType.WARNING));
            }

            final String expr = m_sqlSupplier.get();
            final String ph = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;
            if (expr == null || expr.isBlank() || !expr.contains(ph)) {
                return Optional.of(new TextMessage.Message(
                    "Enter a valid SQL expression containing the '" + ph + "' placeholder.",
                    "", TextMessage.MessageType.WARNING));
            }

            final SparkDataPortObject sparkPort = (SparkDataPortObject) portObjOpt.get();
            final SparkContextID contextID = sparkPort.getContextID();
            final String inputObjectId = sparkPort.getData().getID();

            try {
                final SparkMultiQueryJobInput jobInput =
                    new SparkMultiQueryJobInput(inputObjectId, cols, expr);
                SparkContextUtil
                    .<SparkMultiQueryJobInput, SparkMultiQueryJobOutput>getJobRunFactory(
                        contextID, SparkMultiQueryNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(contextID);

                return Optional.of(new TextMessage.Message(
                    "OK \u2014 " + cols.length + " column(s) validated successfully.",
                    "", TextMessage.MessageType.SUCCESS));

            } catch (final Exception allEx) {
                final List<String> failedCols = new ArrayList<>();
                final List<String> passedCols = new ArrayList<>();
                String lastError = allEx.getMessage();
                if (allEx.getCause() != null && allEx.getCause().getMessage() != null) {
                    lastError = allEx.getCause().getMessage();
                }
                for (final String col : cols) {
                    try {
                        final SparkMultiQueryJobInput ji =
                            new SparkMultiQueryJobInput(inputObjectId, new String[]{col}, expr);
                        SparkContextUtil
                            .<SparkMultiQueryJobInput, SparkMultiQueryJobOutput>getJobRunFactory(
                                contextID, SparkMultiQueryNodeModel.JOB_ID)
                            .createRun(ji)
                            .run(contextID);
                        passedCols.add(col);
                    } catch (final Exception colEx) {
                        failedCols.add(col);
                    }
                }
                final StringBuilder msg = new StringBuilder("Validation failed.");
                if (!failedCols.isEmpty()) msg.append("\nFailed: ").append(String.join(", ", failedCols));
                if (!passedCols.isEmpty()) msg.append("\nPassed: ").append(String.join(", ", passedCols));
                if (lastError != null) msg.append("\n\nError: ").append(lastError);
                return Optional.of(new TextMessage.Message(
                    msg.toString(), "", TextMessage.MessageType.ERROR));
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Selection ──────────────────────────────────────────────────────
    @Layout(DialogSections.ColumnSelectionSection.class)
    @Widget(title = "Target Columns",
        description = "Columns to apply the SQL expression to.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @ValueReference(TargetColumnsRef.class)
    @Persistor(TargetColumnsPersistor.class)
    ColumnFilter m_targetColumns = new ColumnFilter();

    // ── SQL Expression ────────────────────────────────────────────────────────

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "Expression Template",
        description = "Select a preset template to automatically fill the SQL expression field. "
            + "Selecting a non-custom entry replaces the current expression.")
    @Persistor(EphemeralTemplatePersistor.class)
    @ValueReference(TemplateRef.class)
    ExpressionTemplate m_expressionTemplate = ExpressionTemplate.CUSTOM;

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "SQL Expression",
        description = "SQL expression applied to each target column.\n\n"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER
            + " \u2014 placeholder replaced with each target column name.\n\n"
            + "$$variableName \u2014 replaced at execution with the flow variable value. "
            + "STRING variables are automatically single-quoted; INTEGER/DOUBLE are unquoted.\n"
            + "Example: COALESCE(" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + ", $$defaultValue)\n\n"
            + "To insert a flow variable: select from the dropdown below and click Insert.")
    @Persist(configKey = SparkMultiQuerySettings.CFG_SQL_EXPRESSION)
    @ValueReference(SqlExpressionRef.class)
    @ValueProvider(SqlExpressionValueProvider.class)
    String m_sqlExpression = "string(" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + ")";

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "Flow Variable",
        description = "Select a flow variable and click Insert to append $$varName "
            + "at the end of the SQL expression.\n"
            + "STRING variables are single-quoted at execution; INTEGER/DOUBLE are numeric literals.\n"
            + "Note: the Template must be set to '(Custom)' for Insert to take effect.")
    @ChoicesProvider(AllFlowVarsProvider.class)
    @Persistor(EphemeralStringPersistor.class)
    @ValueReference(FlowVarSelectorRef.class)
    String m_flowVarToInsert = "";

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "Insert $$varName",
        description = "Appends $$[variable name] to the SQL expression.")
    @SimpleButtonWidget(ref = InsertFlowVarButtonRef.class, icon = Icon.RELOAD)
    Void m_insertFlowVarButton;

    // ── Output Options ────────────────────────────────────────────────────────
    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Keep original columns",
        description = "Preserve the original target columns and add the transformed columns as new ones. "
            + "Requires the output pattern to differ from '"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + "'.")
    @Persist(configKey = SparkMultiQuerySettings.CFG_KEEP_ORIGINAL)
    @ValueReference(KeepOriginalRef.class)
    boolean m_keepOriginalColumns = false;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Output column pattern",
        description = "Pattern for the output column name. "
            + "Use " + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + " as placeholder.\n"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + " \u2014 replace original column (default)\n"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + "_str \u2014 add new column with suffix")
    @Persist(configKey = SparkMultiQuerySettings.CFG_OUTPUT_PATTERN)
    @ValueReference(OutputPatternRef.class)
    String m_outputColumnPattern = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;

    // ── SQL Preview ───────────────────────────────────────────────────────────
    @Layout(DialogSections.PreviewSection.class)
    @TextMessage(SqlPreviewProvider.class)
    Void m_sqlPreview;

    // ── Validation ────────────────────────────────────────────────────────────
    @Layout(DialogSections.ValidationSection.class)
    @Widget(title = "Run Validation",
        description = "Run a test query (LIMIT 5) to verify the expression against the upstream data. "
            + "Note: $$varName tokens are NOT substituted during dialog validation "
            + "(substitution happens at node execution).")
    @SimpleButtonWidget(ref = CheckButtonRef.class, icon = Icon.RELOAD)
    Void m_checkButton;

    @Layout(DialogSections.ValidationSection.class)
    @TextMessage(ValidationProvider.class)
    Void m_validationDisplay;

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkMultiQueryNodeParameters() {}

    // ── HELPERS ───────────────────────────────────────────────────────────────

    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) return new String[0];
        final ManualFilter mf = filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) return new String[0];
        return mf.m_manuallySelected;
    }
}
