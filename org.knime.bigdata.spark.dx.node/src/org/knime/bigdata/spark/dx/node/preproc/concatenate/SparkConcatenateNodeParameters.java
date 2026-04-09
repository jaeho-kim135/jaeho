package org.knime.bigdata.spark.dx.node.preproc.concatenate;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Concatenate node.
 * Uses ArrayWidget for column mapping definitions and ValueSwitch for unmatched column handling.
 */
@SuppressWarnings("restriction")
class SparkConcatenateNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Mapping",
            description = "Map columns from Left and Right DataFrames. "
                + "Leave 'Matching Column (Right)' empty for automatic same-name mapping.")
        interface MappingSection {}

        @Section(title = "Unmatched Column Handling",
            description = "Choose how to handle columns that are not mapped.")
        @After(MappingSection.class)
        interface UnmatchedSection {}
    }

    // ── ENUMS ──────────────────────────────────────────���──────────────────────

    enum UnmatchedAction {
        @Label("Include (fill with null)") FILL_NULL,
        @Label("Exclude") EXCLUDE;
    }

    // ── COLUMN CHOICES PROVIDERS ────────────────────────────��─────────────────

    /** Left port (port 0) column choices. */
    static final class SparkLeftColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }
    }

    /** Right port (port 1) column choices. */
    static final class SparkRightColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(1)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        }
    }

    // ── COLUMN MAPPING (ArrayWidget element) ───────────────────────────��──────

    /**
     * A single column mapping: Left column name → Right column name.
     * If rightColumn is empty, auto-maps to the Right column with the same name as leftColumn.
     */
    static class ColumnMapping implements NodeParameters {
        @Widget(title = "Column Name (Left)",
            description = "The column from the left (first) input DataFrame.")
        @ChoicesProvider(SparkLeftColumnChoicesProvider.class)
        String m_leftColumn = "";

        @Widget(title = "Matching Column (Right)",
            description = "The column from the right (second) input to map to. "
                + "Leave empty for same-name auto-mapping.")
        @TextInputWidget(placeholder = "Same name auto-mapping")
        String m_rightColumn = "";

        /** Default constructor. */
        ColumnMapping() {}

        /** Constructor with values. */
        ColumnMapping(final String leftColumn, final String rightColumn) {
            m_leftColumn = leftColumn;
            m_rightColumn = rightColumn;
        }
    }

    // ── CUSTOM PERSISTOR ──────────────────────��───────────────────────────────

    /**
     * Bridges ColumnMapping[] to/from parallel String arrays (leftColumns[], rightColumns[]).
     */
    static final class ColumnMappingPersistor implements NodeParametersPersistor<ColumnMapping[]> {

        @Override
        public ColumnMapping[] load(final NodeSettingsRO settings) throws InvalidSettingsException {
            if (settings.containsKey(SparkConcatenateSettings.CFG_LEFT_COLUMNS)) {
                final String[] lefts = settings.getStringArray(SparkConcatenateSettings.CFG_LEFT_COLUMNS);
                final String[] rights = settings.containsKey(SparkConcatenateSettings.CFG_RIGHT_COLUMNS)
                    ? settings.getStringArray(SparkConcatenateSettings.CFG_RIGHT_COLUMNS) : new String[0];
                final ColumnMapping[] mappings = new ColumnMapping[lefts.length];
                for (int i = 0; i < lefts.length; i++) {
                    final String right = (i < rights.length) ? rights[i] : "";
                    mappings[i] = new ColumnMapping(lefts[i], right);
                }
                return mappings;
            }
            return new ColumnMapping[0];
        }

        @Override
        public void save(final ColumnMapping[] obj, final NodeSettingsWO settings) {
            final ColumnMapping[] mappings = (obj != null) ? obj : new ColumnMapping[0];
            final String[] lefts = new String[mappings.length];
            final String[] rights = new String[mappings.length];
            for (int i = 0; i < mappings.length; i++) {
                lefts[i] = mappings[i].m_leftColumn != null ? mappings[i].m_leftColumn : "";
                rights[i] = mappings[i].m_rightColumn != null ? mappings[i].m_rightColumn : "";
            }
            settings.addStringArray(SparkConcatenateSettings.CFG_LEFT_COLUMNS, lefts);
            settings.addStringArray(SparkConcatenateSettings.CFG_RIGHT_COLUMNS, rights);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{
                {SparkConcatenateSettings.CFG_LEFT_COLUMNS},
                {SparkConcatenateSettings.CFG_RIGHT_COLUMNS}
            };
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Mapping ───────────────────────────────────────────────────────

    @Layout(DialogSections.MappingSection.class)
    @Widget(title = "Column mappings",
        description = "Map columns between Left and Right DataFrames. "
            + "Leave 'Matching Column (Right)' empty for same-name auto-mapping. "
            + "One Left column can be mapped to multiple Right columns.")
    @ArrayWidget(elementTitle = "Mapping", addButtonText = "Add Mapping", showSortButtons = false)
    @Persistor(ColumnMappingPersistor.class)
    ColumnMapping[] m_columnMappings = new ColumnMapping[0];

    // ── Unmatched Column Handling ─────────────────────────────────────────────

    @Layout(DialogSections.UnmatchedSection.class)
    @Widget(title = "Unmatched left columns",
        description = "How to handle left columns not present in any mapping. "
            + "'Include' adds them to output with null values for right rows.")
    @ValueSwitchWidget
    @Persist(configKey = SparkConcatenateSettings.CFG_UNMATCHED_LEFT)
    UnmatchedAction m_unmatchedLeftAction = UnmatchedAction.FILL_NULL;

    @Layout(DialogSections.UnmatchedSection.class)
    @Widget(title = "Unmatched right columns",
        description = "How to handle right columns not present in any mapping. "
            + "'Include' adds them to output with null values for left rows. "
            + "If the column name conflicts, a '(1)' suffix is added.")
    @ValueSwitchWidget
    @Persist(configKey = SparkConcatenateSettings.CFG_UNMATCHED_RIGHT)
    UnmatchedAction m_unmatchedRightAction = UnmatchedAction.FILL_NULL;

    // ── CONSTRUCTOR ───────────��──────────────────────────────────��────────────

    SparkConcatenateNodeParameters() {}
}
