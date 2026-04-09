package org.knime.bigdata.spark.dx.node.extract.datetimefields;

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
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.updates.util.BooleanReference;
import org.knime.bigdata.spark.dx.node.LocaleChoicesProvider;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;

/**
 * Node parameters (WebUI dialog settings) for the Spark Extract Date&amp;Time Fields node.
 * Controls which date/time fields to extract and output column naming.
 */
@SuppressWarnings("restriction")
class SparkExtractDateTimeFieldsNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the date/time column to extract fields from.")
        interface ColumnSection {}

        @Section(title = "Date Fields",
            description = "Select which date-related fields to extract.")
        @After(ColumnSection.class)
        interface DateFieldsSection {}

        @Section(title = "Time Fields",
            description = "Select which time-related fields to extract.")
        @After(DateFieldsSection.class)
        interface TimeFieldsSection {}

        @Section(title = "Subsecond",
            description = "Extract subsecond precision fields.")
        @After(TimeFieldsSection.class)
        interface SubsecondSection {}

        @Section(title = "Name Fields",
            description = "Extract locale-dependent name fields (day of week name, month name).")
        @After(SubsecondSection.class)
        interface NameFieldsSection {}

        @Section(title = "Output",
            description = "Configure output column naming.")
        @After(NameFieldsSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum SubsecondUnit {
        @Label("Milliseconds (0-999)") MILLISECOND,
        @Label("Microseconds (0-999,999)") MICROSECOND,
        @Label("Nanoseconds (0-999,999,999)") NANOSECOND;
    }

    // ── PARAMETER REFERENCES ─────────────────────────────────────────────────

    interface ExtractSubsecondRef extends BooleanReference {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    static final class ExtractSubsecondPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getBoolean(ExtractSubsecondRef.class).isTrue();
        }
    }

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

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Selection ─────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnSection.class)
    @Widget(title = "Date/Time Column",
        description = "Select the date/time column to extract fields from.")
    @ChoicesProvider(SparkAllColumnChoicesProvider.class)
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_COLUMN)
    String m_column = "";

    // ── Date Fields ──────────────────────────────────────────────────────────

    @Layout(DialogSections.DateFieldsSection.class)
    @Widget(title = "Year",
        description = "Extract the year (e.g., 2024).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_YEAR)
    boolean m_extractYear = false;

    @Layout(DialogSections.DateFieldsSection.class)
    @Widget(title = "Month (number)",
        description = "Extract the month as a number (1-12).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_MONTH)
    boolean m_extractMonth = false;

    @Layout(DialogSections.DateFieldsSection.class)
    @Widget(title = "Day of month",
        description = "Extract the day of the month (1-31).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_DAY)
    boolean m_extractDay = false;

    @Layout(DialogSections.DateFieldsSection.class)
    @Widget(title = "Day of week (number)",
        description = "Extract the day of week as a number (1=Sunday, 7=Saturday in Spark).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_DAY_OF_WEEK)
    boolean m_extractDayOfWeek = false;

    @Layout(DialogSections.DateFieldsSection.class)
    @Widget(title = "Day of year",
        description = "Extract the day of year (1-366).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_DAY_OF_YEAR)
    boolean m_extractDayOfYear = false;

    @Layout(DialogSections.DateFieldsSection.class)
    @Widget(title = "Week of year",
        description = "Extract the ISO week number of the year (1-53).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_WEEK_OF_YEAR)
    boolean m_extractWeekOfYear = false;

    @Layout(DialogSections.DateFieldsSection.class)
    @Widget(title = "Quarter",
        description = "Extract the quarter of the year (1-4).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_QUARTER)
    boolean m_extractQuarter = false;

    // ── Time Fields ──────────────────────────────────────────────────────────

    @Layout(DialogSections.TimeFieldsSection.class)
    @Widget(title = "Hour",
        description = "Extract the hour (0-23).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_HOUR)
    boolean m_extractHour = false;

    @Layout(DialogSections.TimeFieldsSection.class)
    @Widget(title = "Minute",
        description = "Extract the minute (0-59).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_MINUTE)
    boolean m_extractMinute = false;

    @Layout(DialogSections.TimeFieldsSection.class)
    @Widget(title = "Second",
        description = "Extract the second (0-59).")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_SECOND)
    boolean m_extractSecond = false;

    // ── Subsecond ────────────────────────────────────────────────────────────

    @Layout(DialogSections.SubsecondSection.class)
    @Widget(title = "Subsecond",
        description = "Extract subsecond precision (milliseconds, microseconds, or nanoseconds).")
    @ValueReference(ExtractSubsecondRef.class)
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_SUBSECOND)
    boolean m_extractSubsecond = false;

    @Layout(DialogSections.SubsecondSection.class)
    @Widget(title = "Subsecond Unit",
        description = "The unit of the subsecond field to extract.")
    @ValueSwitchWidget
    @Effect(predicate = ExtractSubsecondPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_SUBSECOND_UNIT)
    SubsecondUnit m_subsecondUnit = SubsecondUnit.MILLISECOND;

    // ── Name Fields ──────────────────────────────────────────────────────────

    @Layout(DialogSections.NameFieldsSection.class)
    @Widget(title = "Day of week (name)",
        description = "Extract the day of week name (e.g., Monday, Tuesday). Locale dependent.")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_DAY_OF_WEEK_NAME)
    boolean m_extractDayOfWeekName = false;

    @Layout(DialogSections.NameFieldsSection.class)
    @Widget(title = "Month (name)",
        description = "Extract the month name (e.g., January, February). Locale dependent.")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_EXTRACT_MONTH_NAME)
    boolean m_extractMonthName = false;

    @Layout(DialogSections.NameFieldsSection.class)
    @Widget(title = "Locale",
        description = "Locale for name fields. Affects day-of-week and month names.")
    @ChoicesProvider(LocaleChoicesProvider.class)
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_LOCALE)
    String m_locale = "en";

    // ── Output ───────────────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Column Name Prefix",
        description = "Prefix for output column names. If empty, no prefix is applied. "
            + "For example, if prefix is 'ts_', the Year column will be named 'ts_Year'.")
    @TextInputWidget(placeholder = "")
    @Persist(configKey = SparkExtractDateTimeFieldsSettings.CFG_COLUMN_PREFIX)
    String m_columnPrefix = "";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkExtractDateTimeFieldsNodeParameters() {
    }
}
