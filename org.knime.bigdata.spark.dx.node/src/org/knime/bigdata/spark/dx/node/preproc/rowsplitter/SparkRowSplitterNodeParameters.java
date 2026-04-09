package org.knime.bigdata.spark.dx.node.preproc.rowsplitter;

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
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;

/**
 * Node parameters (WebUI dialog settings) for the Spark Row Splitter node.
 * Configures filter predicates and match criteria for splitting rows.
 */
@SuppressWarnings("restriction")
class SparkRowSplitterNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Filter Conditions",
            description = "Define the conditions used to split rows into Matches and Non-Matches.")
        interface ConditionsSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum MatchCriteria {
        @Label(value = "AND (all conditions)", description = "All conditions must be true for a row to match.")
        AND,
        @Label(value = "OR (any condition)", description = "At least one condition must be true for a row to match.")
        OR;
    }

    enum FilterOperator {
        @Label("=") EQ,
        @Label("!=") NEQ,
        @Label(">") GT,
        @Label(">=") GTE,
        @Label("<") LT,
        @Label("<=") LTE,
        @Label("BETWEEN") BETWEEN,
        @Label("LIKE") LIKE,
        @Label("REGEX") REGEX,
        @Label("IS NULL") IS_NULL,
        @Label("IS NOT NULL") IS_NOT_NULL,
        @Label("IS TRUE") IS_TRUE,
        @Label("IS FALSE") IS_FALSE;
    }

    // ── COLUMN CHOICES PROVIDER ──────────────────────────────────────────────

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

    // ── PARAMETER REFERENCES (for FilterPredicate @Effect) ─────────────────────

    interface OperatorRef extends ParameterReference<FilterOperator> {}

    // ── EFFECT PREDICATES ─────────────────────────────────────────────────────

    /** Shows m_value when the operator requires a comparison value. */
    static final class OperatorNeedsValuePredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(OperatorRef.class).isOneOf(
                FilterOperator.EQ, FilterOperator.NEQ,
                FilterOperator.GT, FilterOperator.GTE,
                FilterOperator.LT, FilterOperator.LTE,
                FilterOperator.BETWEEN, FilterOperator.LIKE, FilterOperator.REGEX);
        }
    }

    /** Shows m_upperValue only when operator is BETWEEN. */
    static final class OperatorIsBetweenPredicate implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(OperatorRef.class).isOneOf(FilterOperator.BETWEEN);
        }
    }

    // ── FILTER PREDICATE (ArrayWidget element) ────────────────────────────────

    /**
     * A single filter predicate consisting of column, operator, value, upper value,
     * and case sensitivity. Used as the element type of the {@code @ArrayWidget} predicates array.
     */
    static class FilterPredicate implements NodeParameters {
        @Widget(title = "Column", description = "The column to apply the filter on.")
        @ChoicesProvider(SparkColumnChoicesProvider.class)
        String m_column = "";

        @Widget(title = "Operator", description = "The comparison operator.")
        @ValueReference(OperatorRef.class)
        FilterOperator m_operator = FilterOperator.EQ;

        @Widget(title = "Value",
            description = "The value to compare against. Not used for IS NULL, IS NOT NULL, IS TRUE, IS FALSE.")
        @TextInputWidget(placeholder = "Enter value")
        @Effect(predicate = OperatorNeedsValuePredicate.class, type = EffectType.SHOW)
        String m_value = "";

        @Widget(title = "Upper Value",
            description = "The upper bound for BETWEEN operator.")
        @TextInputWidget(placeholder = "Enter upper value")
        @Effect(predicate = OperatorIsBetweenPredicate.class, type = EffectType.SHOW)
        String m_upperValue = "";

        @Widget(title = "Case Sensitive",
            description = "If unchecked, string comparisons are case-insensitive. Only applies to string columns.")
        boolean m_caseSensitive = true;

        /** Default constructor. */
        FilterPredicate() {}

        /** Constructor with values. */
        FilterPredicate(final String column, final FilterOperator operator,
                final String value, final String upperValue, final boolean caseSensitive) {
            m_column = column;
            m_operator = operator;
            m_value = value;
            m_upperValue = upperValue;
            m_caseSensitive = caseSensitive;
        }
    }

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges the FilterPredicate[] array to/from the parallel arrays format
     * used by SparkRowSplitterSettings.
     */
    static final class FilterPredicatesPersistor implements NodeParametersPersistor<FilterPredicate[]> {

        @Override
        public FilterPredicate[] load(final NodeSettingsRO settings) throws InvalidSettingsException {
            if (!settings.containsKey(SparkRowSplitterSettings.CFG_PREDICATE_COUNT)) {
                return new FilterPredicate[]{new FilterPredicate()};
            }
            final int count = settings.getInt(SparkRowSplitterSettings.CFG_PREDICATE_COUNT);
            if (count <= 0) {
                return new FilterPredicate[]{new FilterPredicate()};
            }
            final FilterPredicate[] predicates = new FilterPredicate[count];
            for (int i = 0; i < count; i++) {
                final String prefix = SparkRowSplitterSettings.CFG_PREDICATE_PREFIX + i;
                final NodeSettingsRO sub = settings.getNodeSettings(prefix);
                final String column = sub.getString(SparkRowSplitterSettings.CFG_COLUMN, "");
                final String operatorStr = sub.getString(SparkRowSplitterSettings.CFG_OPERATOR, "EQ");
                final String value = sub.getString(SparkRowSplitterSettings.CFG_VALUE, "");
                final String upperValue = sub.getString(SparkRowSplitterSettings.CFG_UPPER_VALUE, "");
                final boolean caseSensitive = sub.getBoolean(SparkRowSplitterSettings.CFG_CASE_SENSITIVE, true);

                FilterOperator operator;
                try {
                    operator = FilterOperator.valueOf(operatorStr);
                } catch (final IllegalArgumentException e) {
                    operator = FilterOperator.EQ;
                }
                predicates[i] = new FilterPredicate(column, operator, value, upperValue, caseSensitive);
            }
            return predicates;
        }

        @Override
        public void save(final FilterPredicate[] obj, final NodeSettingsWO settings) {
            final FilterPredicate[] predicates = (obj != null) ? obj : new FilterPredicate[0];
            settings.addInt(SparkRowSplitterSettings.CFG_PREDICATE_COUNT, predicates.length);
            for (int i = 0; i < predicates.length; i++) {
                final NodeSettingsWO sub = settings.addNodeSettings(
                    SparkRowSplitterSettings.CFG_PREDICATE_PREFIX + i);
                sub.addString(SparkRowSplitterSettings.CFG_COLUMN,
                    predicates[i].m_column != null ? predicates[i].m_column : "");
                sub.addString(SparkRowSplitterSettings.CFG_OPERATOR,
                    predicates[i].m_operator != null ? predicates[i].m_operator.name() : "EQ");
                sub.addString(SparkRowSplitterSettings.CFG_VALUE,
                    predicates[i].m_value != null ? predicates[i].m_value : "");
                sub.addString(SparkRowSplitterSettings.CFG_UPPER_VALUE,
                    predicates[i].m_upperValue != null ? predicates[i].m_upperValue : "");
                sub.addBoolean(SparkRowSplitterSettings.CFG_CASE_SENSITIVE,
                    predicates[i].m_caseSensitive);
            }
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{
                {SparkRowSplitterSettings.CFG_PREDICATE_COUNT}
            };
        }
    }

    /**
     * Bridges MatchCriteria enum to/from the string format used by SparkRowSplitterSettings.
     */
    static final class MatchCriteriaPersistor implements NodeParametersPersistor<MatchCriteria> {
        @Override
        public MatchCriteria load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(SparkRowSplitterSettings.CFG_MATCH_CRITERIA, "AND");
            try {
                return MatchCriteria.valueOf(val);
            } catch (final IllegalArgumentException e) {
                return MatchCriteria.AND;
            }
        }

        @Override
        public void save(final MatchCriteria obj, final NodeSettingsWO settings) {
            settings.addString(SparkRowSplitterSettings.CFG_MATCH_CRITERIA,
                (obj != null ? obj : MatchCriteria.AND).name());
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SparkRowSplitterSettings.CFG_MATCH_CRITERIA}};
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    @Layout(DialogSections.ConditionsSection.class)
    @Widget(title = "Match Criteria",
        description = "How to combine multiple filter conditions. "
            + "AND requires all conditions to be true; OR requires at least one.")
    @ValueSwitchWidget
    @Persistor(MatchCriteriaPersistor.class)
    MatchCriteria m_matchCriteria = MatchCriteria.AND;

    @Layout(DialogSections.ConditionsSection.class)
    @Widget(title = "Filter Conditions",
        description = "Define one or more filter predicates. Each predicate specifies a column, "
            + "operator, and value. Rows matching these conditions go to the Matches output port; "
            + "non-matching rows go to the Non-Matches output port.")
    @ArrayWidget(elementTitle = "Condition", addButtonText = "Add Condition", showSortButtons = true)
    @Persistor(FilterPredicatesPersistor.class)
    FilterPredicate[] m_predicates = new FilterPredicate[]{new FilterPredicate()};

    // ── CONSTRUCTOR ──────────────────────────────────────────────────────────

    SparkRowSplitterNodeParameters() {}
}
