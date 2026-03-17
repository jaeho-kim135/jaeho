package org.knime.bigdata.spark.dx.node.preproc.cellsplit;

import java.util.List;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;

/**
 * Node parameters (WebUI dialog settings) for the Spark Cell Splitter node.
 * Controls the column to split, delimiter, size mode, and output options.
 */
@SuppressWarnings("restriction")
class SparkCellSplitterNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Split Settings",
            description = "Configure the column to split and the delimiter.")
        interface SplitSection {}

        @Section(title = "Output Columns",
            description = "Configure how the number of output columns is determined.")
        @After(SplitSection.class)
        interface OutputColumnsSection {}

        @Section(title = "Output Options",
            description = "Configure output formatting and column handling.")
        @After(OutputColumnsSection.class)
        interface OutputOptionsSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum SizeMode {
        @Label("Fixed number of columns") FIXED,
        @Label("Auto-detect from data") AUTO;
    }

    // ── COLUMN CHOICES PROVIDER (String columns only) ─────────────────────────

    static final class SparkStringColumnProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream()
                    .filter(cs -> cs.getType().isCompatible(org.knime.core.data.StringValue.class))
                    .toList())
                .orElse(List.of());
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Split Settings ──────────────────────────────────────────────────────

    @Layout(DialogSections.SplitSection.class)
    @Widget(title = "Column to split",
        description = "The string column whose content will be split into multiple columns.")
    @ChoicesProvider(SparkStringColumnProvider.class)
    @Persist(configKey = SparkCellSplitterSettings.CFG_COLUMN)
    String m_column = "";

    @Layout(DialogSections.SplitSection.class)
    @Widget(title = "Delimiter",
        description = "The delimiter used to split the cell content. "
            + "Can be a literal string or a regular expression if 'Use regex' is enabled.")
    @TextInputWidget(placeholder = ",")
    @Persist(configKey = SparkCellSplitterSettings.CFG_DELIMITER)
    String m_delimiter = ",";

    @Layout(DialogSections.SplitSection.class)
    @Widget(title = "Use regular expression as delimiter",
        description = "If checked, the delimiter is interpreted as a Java regular expression.")
    @Persist(configKey = SparkCellSplitterSettings.CFG_USE_REGEX)
    boolean m_useRegex = false;

    // ── Output Columns ──────────────────────────────────────────────────────

    @Layout(DialogSections.OutputColumnsSection.class)
    @Widget(title = "Column count mode",
        description = "How to determine the number of output columns. "
            + "'Fixed' uses a specified count. 'Auto-detect' scans the data to find the maximum number of parts.")
    @ValueSwitchWidget
    @Persist(configKey = SparkCellSplitterSettings.CFG_SIZE_MODE)
    SizeMode m_sizeMode = SizeMode.FIXED;

    @Layout(DialogSections.OutputColumnsSection.class)
    @Widget(title = "Number of output columns",
        description = "The fixed number of output columns to create from the split.")
    @NumberInputWidget
    @Persist(configKey = SparkCellSplitterSettings.CFG_FIXED_SIZE)
    int m_fixedSize = 3;

    @Layout(DialogSections.OutputColumnsSection.class)
    @Widget(title = "Row scan limit",
        description = "Maximum number of rows to scan when auto-detecting the number of output columns. "
            + "A higher value gives more accurate results but takes longer.")
    @NumberInputWidget
    @Persist(configKey = SparkCellSplitterSettings.CFG_SCAN_LIMIT)
    int m_scanLimit = 50000;

    // ── Output Options ──────────────────────────────────────────────────────

    @Layout(DialogSections.OutputOptionsSection.class)
    @Widget(title = "Trim whitespace",
        description = "If checked, leading and trailing whitespace is removed from each split part.")
    @Persist(configKey = SparkCellSplitterSettings.CFG_TRIM)
    boolean m_trim = true;

    @Layout(DialogSections.OutputOptionsSection.class)
    @Widget(title = "Use empty string instead of missing",
        description = "If checked, missing values in the split result are replaced with empty strings. "
            + "Otherwise they remain as null/missing.")
    @Persist(configKey = SparkCellSplitterSettings.CFG_USE_EMPTY_STRING)
    boolean m_useEmptyString = false;

    @Layout(DialogSections.OutputOptionsSection.class)
    @Widget(title = "Remove input column",
        description = "If checked, the original input column is removed from the output.")
    @Persist(configKey = SparkCellSplitterSettings.CFG_REMOVE_INPUT_COL)
    boolean m_removeInputCol = false;

    @Layout(DialogSections.OutputOptionsSection.class)
    @Widget(title = "Output column prefix",
        description = "Prefix for the output column names. If empty, the input column name is used as prefix. "
            + "Output columns are named '&lt;prefix&gt;_1', '&lt;prefix&gt;_2', etc.")
    @TextInputWidget(placeholder = "Leave empty to use input column name")
    @Persist(configKey = SparkCellSplitterSettings.CFG_OUTPUT_PREFIX)
    String m_outputPrefix = "";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkCellSplitterNodeParameters() {
    }
}
