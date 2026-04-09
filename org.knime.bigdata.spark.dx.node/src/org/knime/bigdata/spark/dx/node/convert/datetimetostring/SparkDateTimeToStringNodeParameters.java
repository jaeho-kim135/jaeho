package org.knime.bigdata.spark.dx.node.convert.datetimetostring;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.bigdata.spark.dx.node.LocaleChoicesProvider;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Date&amp;Time to String node.
 * Controls column selection, date format pattern, locale, and append/replace mode.
 */
@SuppressWarnings("restriction")
class SparkDateTimeToStringNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the date/time columns to convert to string.")
        interface ColumnSection {}

        @Section(title = "Format",
            description = "Configure the date/time format pattern and locale.")
        @After(ColumnSection.class)
        interface FormatSection {}

        @Section(title = "Output",
            description = "Configure the output mode (replace or append).")
        @After(FormatSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum AppendOrReplace {
        @Label("Replace selected columns") REPLACE,
        @Label("Append new columns") APPEND;
    }

    // ── PARAMETER REFERENCES ─────────────────────────────────────────────────

    interface AppendOrReplaceRef extends ParameterReference<AppendOrReplace> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    static final class IsAppendPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AppendOrReplaceRef.class).isOneOf(AppendOrReplace.APPEND);
        }
    }

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    /**
     * Provides all columns from the input Spark DataFrame for column filter selection.
     * All columns are listed since Spark date/time types may not always map to
     * specific KNIME date/time value interfaces.
     */
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

    // ── COLUMN FILTER PERSISTOR ───────────────────────────────────────────────

    /**
     * Bridges ColumnFilter to/from the NameFilterConfiguration format used by
     * {@link SparkDateTimeToStringSettings} for selected columns.
     */
    static final class ColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkDateTimeToStringSettings.CFG_COLUMNS;

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

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Selection ────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnSection.class)
    @Widget(title = "Date/Time Columns",
        description = "Select the date/time columns to convert to string.")
    @ColumnFilterWidget(choicesProvider = SparkAllColumnChoicesProvider.class)
    @Persistor(ColumnsPersistor.class)
    ColumnFilter m_columns = new ColumnFilter();

    // ── Format ──────────────────────────────────────────────────────────────

    @Layout(DialogSections.FormatSection.class)
    @Widget(title = "Date Format",
        description = "Java/Spark date format pattern (e.g., yyyy-MM-dd HH:mm:ss). "
            + "See SimpleDateFormat documentation for pattern letters.")
    @TextInputWidget(placeholder = "yyyy-MM-dd HH:mm:ss")
    @Persist(configKey = SparkDateTimeToStringSettings.CFG_FORMAT)
    String m_format = "yyyy-MM-dd HH:mm:ss";

    @Layout(DialogSections.FormatSection.class)
    @Widget(title = "Locale",
        description = "Locale for locale-sensitive formatting such as month/day names.")
    @ChoicesProvider(LocaleChoicesProvider.class)
    @Persist(configKey = SparkDateTimeToStringSettings.CFG_LOCALE)
    String m_locale = "en";

    // ── Output ──────────────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output Mode",
        description = "Replace the selected columns in-place or append new string columns.")
    @ValueSwitchWidget
    @ValueReference(AppendOrReplaceRef.class)
    @Persist(configKey = SparkDateTimeToStringSettings.CFG_APPEND_OR_REPLACE)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.REPLACE;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Column Suffix",
        description = "Suffix appended to the original column name when creating new columns.")
    @TextInputWidget(placeholder = " (String)")
    @Effect(predicate = IsAppendPredicate.class, type = EffectType.SHOW)
    @Persist(configKey = SparkDateTimeToStringSettings.CFG_SUFFIX)
    String m_suffix = " (String)";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkDateTimeToStringNodeParameters() {
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
        final org.knime.core.webui.node.dialog.defaultdialog.setting.filter.util.ManualFilter mf =
            filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) {
            return new String[0];
        }
        return mf.m_manuallySelected;
    }
}
