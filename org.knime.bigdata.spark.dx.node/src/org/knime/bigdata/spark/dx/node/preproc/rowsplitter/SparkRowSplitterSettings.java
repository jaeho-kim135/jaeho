package org.knime.bigdata.spark.dx.node.preproc.rowsplitter;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Row Splitter node.
 * Stores filter predicates (column, operator, value, upperValue, caseSensitive)
 * and match criteria (AND/OR).
 */
public final class SparkRowSplitterSettings {

    // Config keys
    static final String CFG_MATCH_CRITERIA = "matchCriteria";
    static final String CFG_PREDICATE_COUNT = "predicateCount";
    static final String CFG_PREDICATE_PREFIX = "predicate_";
    static final String CFG_COLUMN = "column";
    static final String CFG_OPERATOR = "operator";
    static final String CFG_VALUE = "value";
    static final String CFG_UPPER_VALUE = "upperValue";
    static final String CFG_CASE_SENSITIVE = "caseSensitive";
    static final String CFG_CONFIGURED = "nodeConfigured";

    /** A single filter predicate. */
    public static final class Predicate {
        private final String m_column;
        private final String m_operator;
        private final String m_value;
        private final String m_upperValue;
        private final boolean m_caseSensitive;

        public Predicate(final String column, final String operator, final String value,
                final String upperValue, final boolean caseSensitive) {
            m_column = column;
            m_operator = operator;
            m_value = value;
            m_upperValue = upperValue;
            m_caseSensitive = caseSensitive;
        }

        public String getColumn() { return m_column; }
        public String getOperator() { return m_operator; }
        public String getValue() { return m_value; }
        public String getUpperValue() { return m_upperValue; }
        public boolean isCaseSensitive() { return m_caseSensitive; }
    }

    private String m_matchCriteria = "AND";
    private final List<Predicate> m_predicates = new ArrayList<>();
    private boolean m_nodeConfigured = false;

    /** Creates default settings with one empty predicate. */
    public SparkRowSplitterSettings() {
        m_predicates.add(new Predicate("", "EQ", "", "", true));
    }

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return the match criteria ("AND" or "OR") */
    public String getMatchCriteria() {
        return m_matchCriteria;
    }

    /** @return list of filter predicates */
    public List<Predicate> getPredicates() {
        return m_predicates;
    }

    /** @return true if the node has been configured (dialog was opened and OK'd) */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    // ── Setters ──────────────────────────────────────────────────────────────

    /** @param matchCriteria "AND" or "OR" */
    public void setMatchCriteria(final String matchCriteria) {
        m_matchCriteria = matchCriteria;
    }

    /** @param configured set to true when user accepts the dialog */
    public void setNodeConfigured(final boolean configured) {
        m_nodeConfigured = configured;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_MATCH_CRITERIA, m_matchCriteria);
        settings.addInt(CFG_PREDICATE_COUNT, m_predicates.size());
        for (int i = 0; i < m_predicates.size(); i++) {
            final Predicate pred = m_predicates.get(i);
            final NodeSettingsWO sub = settings.addNodeSettings(CFG_PREDICATE_PREFIX + i);
            sub.addString(CFG_COLUMN, pred.getColumn());
            sub.addString(CFG_OPERATOR, pred.getOperator());
            sub.addString(CFG_VALUE, pred.getValue());
            sub.addString(CFG_UPPER_VALUE, pred.getUpperValue());
            sub.addBoolean(CFG_CASE_SENSITIVE, pred.isCaseSensitive());
        }
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final int count = settings.getInt(CFG_PREDICATE_COUNT);
        if (count <= 0) {
            throw new InvalidSettingsException("At least one filter condition is required.");
        }
        for (int i = 0; i < count; i++) {
            final String prefix = CFG_PREDICATE_PREFIX + i;
            final NodeSettingsRO sub = settings.getNodeSettings(prefix);
            final String column = sub.getString(CFG_COLUMN);
            final String operator = sub.getString(CFG_OPERATOR);

            if (column == null || column.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Condition " + (i + 1) + ": column is not selected.");
            }

            // Validate operator
            try {
                SparkRowSplitterNodeParameters.FilterOperator.valueOf(operator);
            } catch (final IllegalArgumentException e) {
                throw new InvalidSettingsException(
                    "Condition " + (i + 1) + ": invalid operator '" + operator + "'.");
            }

            // Value required for operators other than IS_NULL, IS_NOT_NULL, IS_TRUE, IS_FALSE
            if (!isNullaryOperator(operator)) {
                final String value = sub.getString(CFG_VALUE);
                if (value == null || value.trim().isEmpty()) {
                    throw new InvalidSettingsException(
                        "Condition " + (i + 1) + ": value is empty.");
                }
            }

            // Upper value required for BETWEEN
            if ("BETWEEN".equals(operator)) {
                final String upperValue = sub.getString(CFG_UPPER_VALUE);
                if (upperValue == null || upperValue.trim().isEmpty()) {
                    throw new InvalidSettingsException(
                        "Condition " + (i + 1) + ": upper value is required for BETWEEN operator.");
                }
            }
        }
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_matchCriteria = settings.getString(CFG_MATCH_CRITERIA, "AND");
        m_predicates.clear();

        final int count = settings.getInt(CFG_PREDICATE_COUNT);
        for (int i = 0; i < count; i++) {
            final String prefix = CFG_PREDICATE_PREFIX + i;
            final NodeSettingsRO sub = settings.getNodeSettings(prefix);
            final String column = sub.getString(CFG_COLUMN, "");
            final String operator = sub.getString(CFG_OPERATOR, "EQ");
            final String value = sub.getString(CFG_VALUE, "");
            final String upperValue = sub.getString(CFG_UPPER_VALUE, "");
            final boolean caseSensitive = sub.getBoolean(CFG_CASE_SENSITIVE, true);
            m_predicates.add(new Predicate(column, operator, value, upperValue, caseSensitive));
        }

        // WebUI persistors write field-specific keys (e.g. CFG_PREDICATE_COUNT) but not CFG_CONFIGURED,
        // so we also check for a key that WebUI always writes to detect dialog acceptance.
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED) || settings.containsKey(CFG_PREDICATE_COUNT);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Returns true if the operator does not require a comparison value
     * (IS NULL, IS NOT NULL, IS TRUE, IS FALSE).
     */
    static boolean isNullaryOperator(final String operator) {
        return "IS_NULL".equals(operator)
            || "IS_NOT_NULL".equals(operator)
            || "IS_TRUE".equals(operator)
            || "IS_FALSE".equals(operator);
    }
}
