package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.port.PortObject;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.Icon;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.SimpleButtonWidget;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.ButtonReference;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.updates.util.BooleanReference;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.text.TextAreaWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.message.TextMessage;

/**
 * Node parameters (WebUI dialog settings) for the Spark Rule Engine node.
 * Defines the dialog layout, rule text, default value, and output column settings.
 */
@SuppressWarnings("restriction")
class SparkRuleEngineNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Rule Editor",
            description = "Define rules in the format: condition =&gt; outcome. "
                + "One rule per line. Lines starting with // are comments.")
        interface RuleEditorSection {}

        @Section(title = "Output",
            description = "Configure the output column and default value.")
        @After(RuleEditorSection.class)
        interface OutputSection {}

        @Section(title = "Validation")
        @After(OutputSection.class)
        interface ValidationSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum AppendOrReplace {
        @Label("Append new column") APPEND,
        @Label("Replace existing column") REPLACE;
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface RulesRef extends ParameterReference<String> {}
    interface DefaultIsMissingRef extends BooleanReference {}
    interface DefaultValueRef extends ParameterReference<String> {}
    interface AppendOrReplaceRef extends ParameterReference<AppendOrReplace> {}
    interface OutputColumnNameRef extends ParameterReference<String> {}
    interface ReplaceColumnRef extends ParameterReference<String> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    static final class IsAppendPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.APPEND);
        }
    }

    static final class IsReplacePredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.REPLACE);
        }
    }

    static final class DefaultIsMissingPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getBoolean(DefaultIsMissingRef.class).isTrue();
        }
    }

    /** Button reference for the Evaluate / Validation button. */
    interface EvaluateButtonRef extends ButtonReference {}

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    static final class SparkAllColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }
    }

    // ── VALIDATION STATE PROVIDER ─────────────────────────────────────────────

    /**
     * Runs a validate-only Spark job when the Evaluate button is clicked.
     * Shows a preview of the CASE WHEN result or an error message.
     */
    static final class ValidationProvider
        implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<String> m_rulesSupplier;
        private Supplier<Boolean> m_defaultIsMissingSupplier;
        private Supplier<String> m_defaultValueSupplier;
        private Supplier<AppendOrReplace> m_appendOrReplaceSupplier;
        private Supplier<String> m_outputColumnNameSupplier;
        private Supplier<String> m_replaceColumnSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeOnButtonClick(EvaluateButtonRef.class);
            m_rulesSupplier = initializer.getValueSupplier(RulesRef.class);
            m_defaultIsMissingSupplier = initializer.getValueSupplier(DefaultIsMissingRef.class);
            m_defaultValueSupplier = initializer.getValueSupplier(DefaultValueRef.class);
            m_appendOrReplaceSupplier = initializer.getValueSupplier(AppendOrReplaceRef.class);
            m_outputColumnNameSupplier = initializer.getValueSupplier(OutputColumnNameRef.class);
            m_replaceColumnSupplier = initializer.getValueSupplier(ReplaceColumnRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (!portObjOpt.isPresent()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable validation.",
                    "", TextMessage.MessageType.INFO));
            }

            String rules = m_rulesSupplier.get();
            if (rules == null || rules.trim().isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Enter at least one rule to run validation.",
                    "", TextMessage.MessageType.WARNING));
            }

            SparkDataPortObject sparkPort = (SparkDataPortObject) portObjOpt.get();
            SparkContextID contextID = sparkPort.getContextID();
            String inputObjectId = sparkPort.getData().getID();

            try {
                boolean defaultIsMissing = Boolean.TRUE.equals(m_defaultIsMissingSupplier.get());
                String defaultValue = m_defaultValueSupplier.get();
                if (defaultValue == null) {
                    defaultValue = "";
                }

                AppendOrReplace mode = m_appendOrReplaceSupplier.get();
                boolean isReplace = (mode == AppendOrReplace.REPLACE);
                String outputColumn;
                if (isReplace) {
                    outputColumn = m_replaceColumnSupplier.get();
                } else {
                    outputColumn = m_outputColumnNameSupplier.get();
                }
                if (outputColumn == null || outputColumn.trim().isEmpty()) {
                    outputColumn = "Rule Result";
                }

                String[] ruleLines = rules.split("\\r?\\n");

                SparkRuleEngineJobInput jobInput = new SparkRuleEngineJobInput(
                    inputObjectId,
                    ruleLines, defaultValue, defaultIsMissing,
                    isReplace, outputColumn);

                final ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

                    SparkRuleEngineJobOutput output = SparkContextUtil
                        .<SparkRuleEngineJobInput, SparkRuleEngineJobOutput>getJobRunFactory(
                            contextID, SparkRuleEngineNodeModel.JOB_ID)
                        .createRun(jobInput)
                        .run(contextID);

                    String preview = output.getPreviewData();
                    if (preview != null && !preview.isEmpty()) {
                        return Optional.of(new TextMessage.Message(
                            "Validation succeeded.\n" + preview,
                            "", TextMessage.MessageType.SUCCESS));
                    }

                    return Optional.of(new TextMessage.Message(
                        "Validation succeeded.", "", TextMessage.MessageType.SUCCESS));

                } catch (Exception e) {
                    String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                    return Optional.of(new TextMessage.Message(
                        "Validation failed: " + errMsg,
                        "", TextMessage.MessageType.ERROR));
                } finally {
                    Thread.currentThread().setContextClassLoader(originalCL);
                }
            } catch (Exception e) {
                String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                return Optional.of(new TextMessage.Message(
                    "Validation failed: " + errMsg,
                    "", TextMessage.MessageType.ERROR));
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Rule Editor Section ──────────────────────────────────────────────────

    @Layout(DialogSections.RuleEditorSection.class)
    @Widget(title = "Rules",
        description = "Enter rules, one per line. Format: condition =&gt; outcome. "
            + "Use $column$ for column references. Lines starting with // are comments. "
            + "Example: $age$ &gt; 60 =&gt; \"Senior\"")
    @TextAreaWidget(rows = 8)
    @Persist(configKey = SparkRuleEngineSettings.CFG_RULES)
    @ValueReference(RulesRef.class)
    String m_rules = "";

    // ── Output Section ───────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Default value is MISSING",
        description = "If checked, rows not matching any rule will get a null (missing) value. "
            + "Uncheck to specify a custom default value.")
    @Persist(configKey = SparkRuleEngineSettings.CFG_DEFAULT_IS_MISSING)
    @ValueReference(DefaultIsMissingRef.class)
    boolean m_defaultIsMissing = true;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Default value",
        description = "The default value for rows not matching any rule. "
            + "Use $column$ to reference a column value, or enter a literal string/number.")
    @TextInputWidget(placeholder = "Enter default value or $column$ reference")
    @Effect(predicate = DefaultIsMissingPredicate.class, type = EffectType.HIDE)
    @Persist(configKey = SparkRuleEngineSettings.CFG_DEFAULT_VALUE)
    @ValueReference(DefaultValueRef.class)
    String m_defaultValue = "";

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output mode",
        description = "Append a new column with the rule result, or replace an existing column.")
    @ValueSwitchWidget
    @Persist(configKey = SparkRuleEngineSettings.CFG_APPEND_OR_REPLACE)
    @ValueReference(AppendOrReplaceRef.class)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.APPEND;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output column name",
        description = "Name of the new column to add with the rule result.")
    @TextInputWidget(placeholder = "Rule Result")
    @Effect(predicate = IsAppendPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkRuleEngineSettings.CFG_OUTPUT_COLUMN_NAME)
    @ValueReference(OutputColumnNameRef.class)
    String m_outputColumnName = "Rule Result";

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Replace column",
        description = "The existing column to replace with the rule result.")
    @ChoicesProvider(SparkAllColumnChoicesProvider.class)
    @Effect(predicate = IsReplacePredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkRuleEngineSettings.CFG_REPLACE_COLUMN)
    @ValueReference(ReplaceColumnRef.class)
    String m_replaceColumn = "";

    // ── Validation Section ───────────────────────────────────────────────────

    @Layout(DialogSections.ValidationSection.class)
    @Widget(title = "Evaluate",
        description = "Validate the current rules against the input data and preview the result. "
            + "Requires the upstream node to be executed.")
    @SimpleButtonWidget(ref = EvaluateButtonRef.class, icon = Icon.RELOAD)
    Void m_evaluateButton;

    @Layout(DialogSections.ValidationSection.class)
    @TextMessage(ValidationProvider.class)
    Void m_validationDisplay;

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkRuleEngineNodeParameters() {
    }
}
