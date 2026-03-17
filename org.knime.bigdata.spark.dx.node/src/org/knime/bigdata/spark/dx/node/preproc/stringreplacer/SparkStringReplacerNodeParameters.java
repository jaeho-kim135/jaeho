package org.knime.bigdata.spark.dx.node.preproc.stringreplacer;

import java.util.List;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.StringValue;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;

/**
 * Node parameters (WebUI dialog settings) for the Spark String Replacer node.
 * Controls column selection, pattern type, pattern, replacement, and output settings.
 */
@SuppressWarnings("restriction")
class SparkStringReplacerNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Find & Replace Settings",
            description = "Configure the search pattern and replacement text.")
        interface FindReplaceSection {}

        @Section(title = "Output Settings",
            description = "Configure how the result is written to the output.")
        @After(FindReplaceSection.class)
        interface OutputSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum PatternType {
        @Label("Literal") LITERAL,
        @Label("Wildcard (* ?)") WILDCARD,
        @Label("Regular Expression") REGEX;
    }

    enum ReplacementStrategy {
        @Label("All occurrences") ALL_OCCURRENCES,
        @Label("Whole string match only") WHOLE_STRING;
    }

    enum AppendOrReplace {
        @Label("Replace column") REPLACE,
        @Label("Append new column") APPEND;
    }

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    /**
     * Provides only String-compatible columns from the Spark input port.
     */
    static final class SparkStringColumnProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream()
                    .filter(cs -> cs.getType().isCompatible(StringValue.class))
                    .toList())
                .orElse(List.of());
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Find & Replace Settings ────────────────────────────────────────────

    @Layout(DialogSections.FindReplaceSection.class)
    @Widget(title = "Target column",
        description = "The string column to search and replace in.")
    @ChoicesProvider(SparkStringColumnProvider.class)
    @Persist(configKey = SparkStringReplacerSettings.CFG_COLUMN)
    String m_column = "";

    @Layout(DialogSections.FindReplaceSection.class)
    @Widget(title = "Pattern type",
        description = "The type of pattern matching to use. "
            + "Literal matches exact text. "
            + "Wildcard uses * (any characters) and ? (single character). "
            + "Regular Expression uses Java regex syntax.")
    @ValueSwitchWidget
    @Persist(configKey = SparkStringReplacerSettings.CFG_PATTERN_TYPE)
    PatternType m_patternType = PatternType.LITERAL;

    @Layout(DialogSections.FindReplaceSection.class)
    @Widget(title = "Pattern",
        description = "The search pattern. For Literal mode, this is the exact text to find. "
            + "For Wildcard mode, use * for any characters and ? for a single character. "
            + "For Regex mode, use Java regular expression syntax.")
    @TextInputWidget(placeholder = "Search pattern")
    @Persist(configKey = SparkStringReplacerSettings.CFG_PATTERN)
    String m_pattern = "";

    @Layout(DialogSections.FindReplaceSection.class)
    @Widget(title = "Replacement text",
        description = "The text to replace matches with. "
            + "In Regex mode, backreferences ($1, $2, etc.) are supported.")
    @TextInputWidget(placeholder = "Replacement text")
    @Persist(configKey = SparkStringReplacerSettings.CFG_REPLACEMENT)
    String m_replacement = "";

    @Layout(DialogSections.FindReplaceSection.class)
    @Widget(title = "Case sensitive",
        description = "If checked, matching is case-sensitive. "
            + "If unchecked, upper and lower case characters are treated as equal.")
    @Persist(configKey = SparkStringReplacerSettings.CFG_CASE_SENSITIVE)
    boolean m_caseSensitive = true;

    @Layout(DialogSections.FindReplaceSection.class)
    @Widget(title = "Enable escaping",
        description = "If checked, \\? matches a literal question mark and \\\\ matches a literal backslash "
            + "in wildcard patterns. Only available in Wildcard mode.")
    @Persist(configKey = SparkStringReplacerSettings.CFG_ENABLE_ESCAPING)
    boolean m_enableEscaping = false;

    @Layout(DialogSections.FindReplaceSection.class)
    @Widget(title = "Replacement strategy",
        description = "All occurrences: replaces every match within each cell. "
            + "Whole string match only: replaces the entire cell value only if it completely matches the pattern.")
    @ValueSwitchWidget
    @Persist(configKey = SparkStringReplacerSettings.CFG_REPLACEMENT_STRATEGY)
    ReplacementStrategy m_replacementStrategy = ReplacementStrategy.WHOLE_STRING;

    // ── Output Settings ─────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Output mode",
        description = "Replace column: overwrites the selected column. "
            + "Append new column: keeps the original and adds a new column with the result.")
    @ValueSwitchWidget
    @Persist(configKey = SparkStringReplacerSettings.CFG_APPEND_OR_REPLACE)
    AppendOrReplace m_appendOrReplace = AppendOrReplace.REPLACE;

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "New column name",
        description = "Name of the new column to append with the replacement result.")
    @TextInputWidget(placeholder = "Replaced")
    @Persist(configKey = SparkStringReplacerSettings.CFG_NEW_COL_NAME)
    String m_newColName = "Replaced";

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkStringReplacerNodeParameters() {
    }
}
