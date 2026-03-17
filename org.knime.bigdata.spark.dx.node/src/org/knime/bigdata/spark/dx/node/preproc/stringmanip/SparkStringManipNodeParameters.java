package org.knime.bigdata.spark.dx.node.preproc.stringmanip;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.ButtonReference;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.text.TextAreaWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.widget.message.TextMessage;
import org.knime.core.node.port.PortObject;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.Icon;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.SimpleButtonWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark String Manipulation node.
 * A single Spark SQL string expression is applied to produce a new or replaced column.
 */
@SuppressWarnings("restriction")
class SparkStringManipNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Expression",
            description = "Enter a Spark SQL string expression. "
                + "Available functions: initcap, lower, upper, trim, ltrim, rtrim, "
                + "length, reverse, substring, locate, replace, lpad, rpad, "
                + "concat, concat_ws, regexp_replace, regexp_extract, cast.")
        interface ExpressionSection {}

        @Section(title = "Output",
            description = "Configure the output column.")
        @After(ExpressionSection.class)
        interface OutputSection {}

        @Section(title = "Evaluate",
            description = "Validate and preview the expression result.")
        @After(OutputSection.class)
        interface EvaluateSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum AppendOrReplace {
        @Label("Append new column") APPEND,
        @Label("Replace existing column") REPLACE;
    }

    // ── EFFECT PREDICATES ───────────────────────────────────────────────────────

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

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface ExpressionRef extends ParameterReference<String> {}
    interface AppendOrReplaceRef extends ParameterReference<AppendOrReplace> {}
    interface OutputColNameRef extends ParameterReference<String> {}
    interface ReplaceColumnRef extends ParameterReference<String> {}

    /** Button reference for the Evaluate button. */
    interface EvaluateRef extends ButtonReference {}

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

    // ── EVALUATE STATE PROVIDER ───────────────────────────────────────────────

    /**
     * Runs a validate-only Spark job when the Evaluate button is clicked.
     * Shows a preview of the expression result or an error message.
     */
    static final class EvaluateResultProvider
        implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<String> m_expressionSupplier;
        private Supplier<AppendOrReplace> m_modeSupplier;
        private Supplier<String> m_outputColNameSupplier;
        private Supplier<String> m_replaceColumnSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeOnButtonClick(EvaluateRef.class);
            m_expressionSupplier = initializer.getValueSupplier(ExpressionRef.class);
            m_modeSupplier = initializer.getValueSupplier(AppendOrReplaceRef.class);
            m_outputColNameSupplier = initializer.getValueSupplier(OutputColNameRef.class);
            m_replaceColumnSupplier = initializer.getValueSupplier(ReplaceColumnRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (portObjOpt.isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable evaluation.",
                    "", TextMessage.MessageType.INFO));
            }

            String expression = m_expressionSupplier.get();
            if (expression == null || expression.trim().isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Enter a Spark SQL expression to evaluate.",
                    "", TextMessage.MessageType.WARNING));
            }

            AppendOrReplace mode = m_modeSupplier.get();
            String outputColName;
            if (mode == AppendOrReplace.REPLACE) {
                outputColName = m_replaceColumnSupplier.get();
                if (outputColName == null || outputColName.trim().isEmpty()) {
                    return Optional.of(new TextMessage.Message(
                        "Select a column to replace.",
                        "", TextMessage.MessageType.WARNING));
                }
            } else {
                outputColName = m_outputColNameSupplier.get();
                if (outputColName == null || outputColName.trim().isEmpty()) {
                    return Optional.of(new TextMessage.Message(
                        "Enter an output column name.",
                        "", TextMessage.MessageType.WARNING));
                }
            }

            SparkDataPortObject sparkPort = (SparkDataPortObject) portObjOpt.get();
            SparkContextID contextID = sparkPort.getContextID();
            String inputObjectId = sparkPort.getData().getID();

            try {
                SparkStringManipJobInput jobInput = new SparkStringManipJobInput(
                    inputObjectId,
                    expression,
                    mode.name(),
                    outputColName,
                    mode == AppendOrReplace.REPLACE ? outputColName : "");

                SparkStringManipJobOutput output = SparkContextUtil
                    .<SparkStringManipJobInput, SparkStringManipJobOutput>getJobRunFactory(
                        contextID, SparkStringManipNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(contextID);

                String preview = output.getPreviewData();
                if (preview != null && !preview.isEmpty()) {
                    return Optional.of(new TextMessage.Message(
                        "Expression is valid.\n" + preview,
                        "", TextMessage.MessageType.SUCCESS));
                }
                return Optional.of(new TextMessage.Message(
                    "Expression is valid.",
                    "", TextMessage.MessageType.SUCCESS));

            } catch (Exception e) {
                String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                return Optional.of(new TextMessage.Message(
                    "Evaluation failed: " + errMsg,
                    "", TextMessage.MessageType.ERROR));
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Expression ────────────────────────────────────────────────────────────

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "Expression",
        description = "A Spark SQL expression for string manipulation. "
            + "Use backtick-quoted column names (e.g. `col_name`). "
            + "Examples: upper(`name`), concat(`first`, ' ', `last`), "
            + "regexp_replace(`text`, '[0-9]', ''), substring(`code`, 1, 3).")
    @TextAreaWidget(rows = 5)
    @Persist(configKey = SparkStringManipSettings.CFG_EXPRESSION)
    @ValueReference(ExpressionRef.class)
    String m_expression = "";

    // ── Output ────────────────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output mode",
        description = "Append a new column or replace an existing column with the expression result.")
    @ValueSwitchWidget
    @Persist(configKey = SparkStringManipSettings.CFG_APPEND_OR_REPLACE)
    @ValueReference(AppendOrReplaceRef.class)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.APPEND;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output column name",
        description = "Name of the new column to append.")
    @TextInputWidget(placeholder = "StringManip")
    @Effect(predicate = IsAppendPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkStringManipSettings.CFG_OUTPUT_COL_NAME)
    @ValueReference(OutputColNameRef.class)
    String m_outputColName = "StringManip";

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Replace column",
        description = "The existing column to replace with the expression result.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Effect(predicate = IsReplacePredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkStringManipSettings.CFG_REPLACE_COLUMN)
    @ValueReference(ReplaceColumnRef.class)
    String m_replaceColumn = "";

    // ── Evaluate ──────────────────────────────────────────────────────────────

    @Layout(DialogSections.EvaluateSection.class)
    @Widget(title = "Evaluate",
        description = "Validate the expression and show a preview of the result. "
            + "Requires the upstream node to be executed.")
    @SimpleButtonWidget(ref = EvaluateRef.class, icon = Icon.RELOAD)
    Void m_evaluateButton;

    @Layout(DialogSections.EvaluateSection.class)
    @TextMessage(EvaluateResultProvider.class)
    Void m_evaluateResult;

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkStringManipNodeParameters() {
    }
}
