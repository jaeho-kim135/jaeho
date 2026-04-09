package org.knime.bigdata.spark.dx.node.calculate.datetimediff;

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
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;

/**
 * Node parameters (WebUI dialog settings) for the Spark Date&Time Difference node.
 * Calculates the difference between two date/time values with configurable granularity.
 */
@SuppressWarnings("restriction")
class SparkDateTimeDifferenceNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "First Date/Time Column",
            description = "Select the first date/time column for the difference calculation.")
        interface FirstColumnSection {}

        @Section(title = "Second Date/Time Value",
            description = "Configure the second date/time value source: another column, a fixed value, or the current timestamp.")
        @After(FirstColumnSection.class)
        interface SecondValueSection {}

        @Section(title = "Difference Options",
            description = "Configure the direction and granularity of the difference calculation.")
        @After(SecondValueSection.class)
        interface OptionsSection {}

        @Section(title = "Output",
            description = "Configure the output column name.")
        @After(OptionsSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum SecondMode {
        @Label("Second column") COLUMN,
        @Label("Fixed date/time") FIXED,
        @Label("Current timestamp") CURRENT;
    }

    enum Direction {
        @Label("Second - First") SECOND_MINUS_FIRST,
        @Label("First - Second") FIRST_MINUS_SECOND;
    }

    enum Granularity {
        @Label("Years") YEAR,
        @Label("Months") MONTH,
        @Label("Weeks") WEEK,
        @Label("Days") DAY,
        @Label("Hours") HOUR,
        @Label("Minutes") MINUTE,
        @Label("Seconds") SECOND,
        @Label("Milliseconds") MILLISECOND,
        @Label("Microseconds") MICROSECOND;
    }

    // ── PARAMETER REFERENCES ─────────────────────────────────────────────────

    interface SecondModeRef extends ParameterReference<SecondMode> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    static final class IsColumnPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(SecondModeRef.class).isOneOf(SecondMode.COLUMN);
        }
    }

    static final class IsFixedPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(SecondModeRef.class).isOneOf(SecondMode.FIXED);
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
                .orElse(Collections.<DataColumnSpec>emptyList());
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── First Column ─────────────────────────────────────────────────────────

    @Layout(DialogSections.FirstColumnSection.class)
    @Widget(title = "First date/time column",
        description = "The first date/time column for the difference calculation.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Persist(configKey = SparkDateTimeDifferenceSettings.CFG_FIRST_COLUMN)
    String m_firstColumn = "";

    // ── Second Value ─────────────────────────────────────────────────────────

    @Layout(DialogSections.SecondValueSection.class)
    @Widget(title = "Second value source",
        description = "How to obtain the second date/time value: "
            + "from another column, a fixed date/time string, or the current timestamp.")
    @RadioButtonsWidget
    @ValueReference(SecondModeRef.class)
    @Persist(configKey = SparkDateTimeDifferenceSettings.CFG_SECOND_MODE)
    SecondMode m_secondMode = SecondMode.COLUMN;

    @Layout(DialogSections.SecondValueSection.class)
    @Widget(title = "Second date/time column",
        description = "The second date/time column for the difference calculation.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Effect(predicate = IsColumnPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDateTimeDifferenceSettings.CFG_SECOND_COLUMN)
    String m_secondColumn = "";

    @Layout(DialogSections.SecondValueSection.class)
    @Widget(title = "Fixed date/time value",
        description = "A fixed date/time string (e.g., '2024-01-01', '2024-01-01 12:00:00'). "
            + "Supported formats: 'yyyy-MM-dd' for date, 'yyyy-MM-dd HH:mm:ss' for timestamp.")
    @TextInputWidget(placeholder = "yyyy-MM-dd HH:mm:ss")
    @Effect(predicate = IsFixedPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDateTimeDifferenceSettings.CFG_FIXED_DATE_TIME)
    String m_fixedDateTime = "";

    // ── Difference Options ───────────────────────────────────────────────────

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Difference direction",
        description = "The direction of the subtraction. "
            + "'Second - First' computes (second value - first column). "
            + "'First - Second' computes (first column - second value).")
    @ValueSwitchWidget
    @Persist(configKey = SparkDateTimeDifferenceSettings.CFG_DIRECTION)
    Direction m_direction = Direction.SECOND_MINUS_FIRST;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Granularity",
        description = "The unit for the difference result. "
            + "For Years/Months/Weeks/Days, the result is an integer. "
            + "For Hours/Minutes, the result is a double. "
            + "For Seconds/Milliseconds/Microseconds, the result is a long.")
    @RadioButtonsWidget
    @Persist(configKey = SparkDateTimeDifferenceSettings.CFG_GRANULARITY)
    Granularity m_granularity = Granularity.DAY;

    // ── Output ───────────────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output column name",
        description = "The name of the new column that will contain the computed difference.")
    @TextInputWidget(placeholder = "Difference")
    @Persist(configKey = SparkDateTimeDifferenceSettings.CFG_OUTPUT_COL_NAME)
    String m_outputColName = "Difference";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkDateTimeDifferenceNodeParameters() {
    }
}
