package org.knime.bigdata.spark.dx.node.preproc.constantvalue;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.updates.util.BooleanReference;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;

/**
 * Node parameters (WebUI dialog settings) for the Spark Constant Value Column node.
 * Controls constant value type, value, missing flag, and append/replace mode.
 */
@SuppressWarnings("restriction")
class SparkConstantValueColumnNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Value Settings",
            description = "Configure the constant value type and value.")
        interface ValueSection {}

        @Section(title = "Output Settings",
            description = "Configure the output column name or replacement target.")
        @After(ValueSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum ValueType {
        @Label("String") STRING,
        @Label("Integer") INTEGER,
        @Label("Long") LONG,
        @Label("Double") DOUBLE,
        @Label("Boolean") BOOLEAN,
        @Label("Date") DATE,
        @Label("Timestamp") TIMESTAMP;
    }

    enum AppendOrReplace {
        @Label("Append") APPEND,
        @Label("Replace") REPLACE;
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface IsMissingRef extends BooleanReference {}
    interface AppendOrReplaceRef extends ParameterReference<AppendOrReplace> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    /** Hides m_value when m_isMissing is true. */
    static final class IsMissingPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getBoolean(IsMissingRef.class).isTrue();
        }
    }

    /** Shows m_columnName when output mode is APPEND. */
    static final class IsAppendPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.APPEND);
        }
    }

    /** Shows m_replaceColumn when output mode is REPLACE. */
    static final class IsReplacePredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.REPLACE);
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

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Value Settings ──────────────────────────────────────────────────────

    @Layout(DialogSections.ValueSection.class)
    @Widget(title = "Value type",
        description = "The data type of the constant value.")
    @RadioButtonsWidget
    @Persist(configKey = SparkConstantValueColumnSettings.CFG_VALUE_TYPE)
    ValueType m_valueType = ValueType.STRING;

    @Layout(DialogSections.ValueSection.class)
    @Widget(title = "Use missing value",
        description = "If checked, the column will be filled with missing (null) values of the selected type.")
    @Persist(configKey = SparkConstantValueColumnSettings.CFG_IS_MISSING)
    @ValueReference(IsMissingRef.class)
    boolean m_isMissing = false;

    @Layout(DialogSections.ValueSection.class)
    @Widget(title = "Value",
        description = "The constant value to fill the column with. "
            + "For Date, use 'yyyy-MM-dd' format. For Timestamp, use 'yyyy-MM-dd HH:mm:ss' format.")
    @TextInputWidget(placeholder = "Enter constant value")
    @Effect(predicate = IsMissingPredicate.class, type = EffectType.HIDE)
    @Persist(configKey = SparkConstantValueColumnSettings.CFG_VALUE)
    String m_value = "";

    // ── Output Settings ─────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output mode",
        description = "Append a new column or replace an existing column.")
    @ValueSwitchWidget
    @Persist(configKey = SparkConstantValueColumnSettings.CFG_APPEND_OR_REPLACE)
    @ValueReference(AppendOrReplaceRef.class)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.APPEND;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "New column name",
        description = "Name of the new column to append.")
    @TextInputWidget(placeholder = "constant")
    @Effect(predicate = IsAppendPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkConstantValueColumnSettings.CFG_COLUMN_NAME)
    String m_columnName = "constant";

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Replace column",
        description = "The existing column to replace with the constant value.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Effect(predicate = IsReplacePredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkConstantValueColumnSettings.CFG_REPLACE_COLUMN)
    String m_replaceColumn = "";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkConstantValueColumnNodeParameters() {
    }
}
