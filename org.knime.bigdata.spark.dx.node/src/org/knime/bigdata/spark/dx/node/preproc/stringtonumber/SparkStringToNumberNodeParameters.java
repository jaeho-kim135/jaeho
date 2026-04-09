package org.knime.bigdata.spark.dx.node.preproc.stringtonumber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.def.StringCell;
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
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark String to Number node.
 */
@SuppressWarnings("restriction")
class SparkStringToNumberNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the String columns to convert to numbers.")
        interface ColumnSelectionSection {}

        @Section(title = "Parsing Options",
            description = "Configure how string values are parsed into numbers.")
        @After(ColumnSelectionSection.class)
        interface ParsingOptionsSection {}
    }

    // ── PARSE TYPE ENUM ──────────────────────────────────────────────────────

    enum ParseTypeOption {
        @Label(value = "Number (Integer)", description = "Convert to 32-bit integer.")
        INTEGER,
        @Label(value = "Number (Double)", description = "Convert to 64-bit floating point.")
        DOUBLE,
        @Label(value = "Number (Long)", description = "Convert to 64-bit integer.")
        LONG;
    }

    // ── COLUMN CHOICES PROVIDER ──────────────────────────────────────────────

    /**
     * Provides only String-type columns from the Spark input spec.
     */
    static final class SparkStringColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream()
                    .filter(colSpec -> colSpec.getType().isCompatible(
                        org.knime.core.data.StringValue.class))
                    .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }
    }

    // ── PERSISTORS ───────────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter to/from the settings under key "include".
     */
    static final class IncludedColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkStringToNumberSettings.CFG_INCLUDE;

        IncludedColumnsPersistor() {
            super(KEY);
        }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return loadColumnFilterWithFallback(settings, KEY);
        }
    }

    /**
     * Bridges ParseTypeOption enum to/from the string format used by Settings.
     */
    static final class ParseTypePersistor implements NodeParametersPersistor<ParseTypeOption> {
        private static final String CFG_KEY = SparkStringToNumberSettings.CFG_PARSE_TYPE;

        @Override
        public ParseTypeOption load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(CFG_KEY, "DOUBLE");
            if ("INTEGER".equals(val)) {
                return ParseTypeOption.INTEGER;
            } else if ("LONG".equals(val)) {
                return ParseTypeOption.LONG;
            } else {
                return ParseTypeOption.DOUBLE;
            }
        }

        @Override
        public void save(final ParseTypeOption obj, final NodeSettingsWO settings) {
            final ParseTypeOption effective = obj != null ? obj : ParseTypeOption.DOUBLE;
            final String val;
            if (effective == ParseTypeOption.INTEGER) {
                val = "INTEGER";
            } else if (effective == ParseTypeOption.LONG) {
                val = "LONG";
            } else {
                val = "DOUBLE";
            }
            settings.addString(CFG_KEY, val);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    // ── HELPER METHODS ───────────────────────────────────────────────────────

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

    // ── FIELDS ───────────────────────────────────────────────────────────────

    // ── Column Selection ─────────────────────────────────────────────────────
    @Layout(DialogSections.ColumnSelectionSection.class)
    @Widget(title = "Included Columns",
        description = "String columns to convert to numbers.")
    @ColumnFilterWidget(choicesProvider = SparkStringColumnChoicesProvider.class)
    @Persistor(IncludedColumnsPersistor.class)
    ColumnFilter m_inclCols = new ColumnFilter();

    // ── Parsing Options ──────────────────────────────────────────────────────
    @Layout(DialogSections.ParsingOptionsSection.class)
    @Widget(title = "Type",
        description = "The target numeric type for the conversion.")
    @Persistor(ParseTypePersistor.class)
    ParseTypeOption m_parseType = ParseTypeOption.DOUBLE;

    @Layout(DialogSections.ParsingOptionsSection.class)
    @Widget(title = "Decimal separator",
        description = "Character used as decimal point. Leave empty for default ('.').")
    @Persist(configKey = SparkStringToNumberSettings.CFG_DECIMAL_SEPARATOR)
    String m_decimalSep = ".";

    @Layout(DialogSections.ParsingOptionsSection.class)
    @Widget(title = "Thousands separator",
        description = "Character used as thousands grouping separator. Leave empty to disable.")
    @Persist(configKey = SparkStringToNumberSettings.CFG_THOUSANDS_SEPARATOR)
    String m_thousandsSep = "";

    @Layout(DialogSections.ParsingOptionsSection.class)
    @Widget(title = "Accept type suffix (d, D, f, F)",
        description = "If checked, numeric strings ending with d/D/f/F suffixes are accepted.")
    @Persist(configKey = SparkStringToNumberSettings.CFG_GENERIC_PARSE)
    boolean m_genericParse = false;

    @Layout(DialogSections.ParsingOptionsSection.class)
    @Widget(title = "Fail on error",
        description = "If checked, the node fails when a value cannot be converted. "
            + "Otherwise, unconvertible values become missing (null).")
    @Persist(configKey = SparkStringToNumberSettings.CFG_FAIL_ON_ERROR)
    boolean m_failOnError = false;

    // ── CONSTRUCTOR ──────────────────────────────────────────────────────────

    SparkStringToNumberNodeParameters() {}

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
