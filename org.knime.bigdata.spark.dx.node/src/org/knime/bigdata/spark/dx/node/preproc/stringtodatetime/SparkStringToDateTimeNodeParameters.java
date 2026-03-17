package org.knime.bigdata.spark.dx.node.preproc.stringtodatetime;

import java.util.List;
import java.util.Locale;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark String to Date&amp;Time node.
 */
@SuppressWarnings("restriction")
class SparkStringToDateTimeNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the string columns to convert to date&amp;time.")
        interface ColumnSelectionSection {}

        @Section(title = "Type and Format",
            description = "Configure the output type, format pattern, and locale.")
        interface TypeAndFormatSection {}
    }

    // ── OUTPUT TYPE ENUM ─────────────────────────────────────────────────────

    enum OutputTypeOption {
        DATE("Date"),
        TIME("Time"),
        DATE_TIME("Date&amp;Time"),
        ZONED_DATE_TIME("Zoned Date&amp;Time");

        private final String m_label;

        OutputTypeOption(final String label) {
            m_label = label;
        }

        @Override
        public String toString() {
            return m_label;
        }
    }

    // ── COLUMN CHOICES PROVIDER ──────────────────────────────────────────────

    /**
     * Provides only String-compatible columns from the Spark input spec.
     */
    static final class SparkStringColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream()
                    .filter(colSpec -> colSpec.getType().isCompatible(StringValue.class))
                    .toList())
                .orElse(List.of());
        }
    }

    // ── PERSISTORS ───────────────────────────────────────────────────────────

    static final class IncludedColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkStringToDateTimeSettings.CFG_INCLUDE;

        IncludedColumnsPersistor() {
            super(KEY);
        }

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

    static final class OutputTypePersistor implements NodeParametersPersistor<OutputTypeOption> {
        private static final String CFG_KEY = SparkStringToDateTimeSettings.CFG_OUTPUT_TYPE;

        @Override
        public OutputTypeOption load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String s = settings.getString(CFG_KEY, "DATE");
            try {
                return OutputTypeOption.valueOf(s);
            } catch (final IllegalArgumentException e) {
                return OutputTypeOption.DATE;
            }
        }

        @Override
        public void save(final OutputTypeOption value, final NodeSettingsWO settings) {
            settings.addString(CFG_KEY, value != null ? value.name() : "DATE");
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    static final class FormatPersistor implements NodeParametersPersistor<String> {
        private static final String CFG_KEY = SparkStringToDateTimeSettings.CFG_FORMAT;

        @Override
        public String load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return settings.getString(CFG_KEY, "yyyy-MM-dd");
        }

        @Override
        public void save(final String value, final NodeSettingsWO settings) {
            settings.addString(CFG_KEY, value != null ? value : "yyyy-MM-dd");
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    static final class LocalePersistor implements NodeParametersPersistor<String> {
        private static final String CFG_KEY = SparkStringToDateTimeSettings.CFG_LOCALE;

        @Override
        public String load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return settings.getString(CFG_KEY, Locale.getDefault().toLanguageTag());
        }

        @Override
        public void save(final String value, final NodeSettingsWO settings) {
            settings.addString(CFG_KEY, value != null ? value : Locale.getDefault().toLanguageTag());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    static final class FailOnErrorPersistor implements NodeParametersPersistor<Boolean> {
        private static final String CFG_KEY = SparkStringToDateTimeSettings.CFG_FAIL_ON_ERROR;

        @Override
        public Boolean load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return settings.getBoolean(CFG_KEY, false);
        }

        @Override
        public void save(final Boolean value, final NodeSettingsWO settings) {
            settings.addBoolean(CFG_KEY, value != null ? value : false);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    // ── FIELDS ───────────────────────────────────────────────────────────────

    @Layout(DialogSections.ColumnSelectionSection.class)
    @Widget(title = "Included Columns",
        description = "String columns to convert to date&amp;time.")
    @ColumnFilterWidget(choicesProvider = SparkStringColumnChoicesProvider.class)
    @Persistor(IncludedColumnsPersistor.class)
    ColumnFilter m_inclCols = new ColumnFilter();

    @Layout(DialogSections.TypeAndFormatSection.class)
    @Widget(title = "Output type",
        description = "The type of the output columns: Date, Time, Date&amp;Time, or Zoned Date&amp;Time.")
    @ValueSwitchWidget
    @Persistor(OutputTypePersistor.class)
    OutputTypeOption m_outputType = OutputTypeOption.DATE;

    @Layout(DialogSections.TypeAndFormatSection.class)
    @Widget(title = "Date format",
        description = """
            A format string that defines the expected format of the input strings. \
            Common examples: "yyyy-MM-dd" for dates, "HH:mm:ss" for times, \
            "yyyy-MM-dd HH:mm:ss" for date&amp;time. \
            Uses Java DateTimeFormatter pattern syntax.""")
    @Persistor(FormatPersistor.class)
    String m_format = "yyyy-MM-dd";

    @Layout(DialogSections.TypeAndFormatSection.class)
    @Widget(title = "Locale",
        description = """
            A locale language tag (e.g., "en-US", "de-DE", "ko-KR") that determines \
            the language for month and weekday names. \
            Note: Locale support in Spark is limited to Spark's built-in date parsing.""")
    @Persistor(LocalePersistor.class)
    String m_locale = Locale.getDefault().toLanguageTag();

    @Layout(DialogSections.TypeAndFormatSection.class)
    @Widget(title = "Fail on error",
        description = """
            If checked, the node will abort execution when a string cannot be parsed. \
            If unchecked, missing values will be generated for unparseable strings.""")
    @Persistor(FailOnErrorPersistor.class)
    boolean m_failOnError = false;

    // ── CONSTRUCTOR ──────────────────────────────────────────────────────────

    SparkStringToDateTimeNodeParameters() {}

    // ── HELPERS ──────────────────────────────────────────────────────────────

    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) return new String[0];
        final org.knime.core.webui.node.dialog.defaultdialog.setting.filter.util.ManualFilter mf =
            filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) return new String[0];
        return mf.m_manuallySelected;
    }
}
