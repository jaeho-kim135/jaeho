package org.knime.bigdata.spark.dx.node.sql.multiquery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersInputImpl;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.Icon;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.SimpleButtonWidget;
import org.knime.core.webui.node.dialog.defaultdialog.setting.filter.util.ManualFilter;
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
import org.knime.node.parameters.updates.ButtonReference;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.FlowVariableChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.widget.message.TextMessage;

/**
 * Node parameters (WebUI dialog settings) for the Spark Multi Query node.
 */
@SuppressWarnings("restriction")
class SparkMultiQueryNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Column Selection",
            description = "Select the target columns to apply the SQL expression to.")
        interface ColumnSelectionSection {}

        @Section(title = "SQL Expression",
            description = "Select a template or enter a custom SQL expression.")
        @After(ColumnSelectionSection.class)
        interface ExpressionSection {}

        @Section(title = "Output Options")
        @After(ExpressionSection.class)
        interface OptionsSection {}

        @Section(title = "SQL Preview",
            description = "Live preview of the SELECT statement that will be generated.")
        @After(OptionsSection.class)
        interface PreviewSection {}

        @Section(title = "Validation",
            description = "Run a test query against the upstream Spark data.")
        @After(PreviewSection.class)
        interface ValidationSection {}
    }

    // ── EXPRESSION TEMPLATE ENUM ──────────────────────────────────────────────

    enum ExpressionTemplate {
        // ── General ──
        @Label(value = "(Custom)", description = "Enter a custom SQL expression.")
        CUSTOM,

        // ── String ──
        @Label(value = "Uppercase", description = "UPPER($columnS)") UPPERCASE,
        @Label(value = "Lowercase", description = "LOWER($columnS)") LOWERCASE,
        @Label(value = "Trim", description = "TRIM($columnS)") TRIM,
        @Label(value = "Left Trim", description = "LTRIM($columnS)") LTRIM,
        @Label(value = "Right Trim", description = "RTRIM($columnS)") RTRIM,
        @Label(value = "String Length", description = "LENGTH($columnS)") STR_LENGTH,
        @Label(value = "Capitalize Words", description = "INITCAP($columnS)") INITCAP,
        @Label(value = "Reverse String", description = "REVERSE($columnS)") REVERSE,
        @Label(value = "Concat", description = "CONCAT($columnS, '')") CONCAT,
        @Label(value = "Concat with Separator", description = "CONCAT_WS(',', $columnS, '')") CONCAT_WS,
        @Label(value = "Substring", description = "SUBSTRING($columnS, 1, 5)") SUBSTRING,
        @Label(value = "Substring by Delimiter", description = "SUBSTRING_INDEX($columnS, ',', 1)") SUBSTRING_INDEX,
        @Label(value = "Replace", description = "REPLACE($columnS, 'old', 'new')") STR_REPLACE,
        @Label(value = "Translate Characters", description = "TRANSLATE($columnS, 'abc', 'xyz')") TRANSLATE,
        @Label(value = "Left Pad", description = "LPAD($columnS, 10, '0')") LPAD,
        @Label(value = "Right Pad", description = "RPAD($columnS, 10, ' ')") RPAD,
        @Label(value = "Find Position (INSTR)", description = "INSTR($columnS, 'search')") INSTR,
        @Label(value = "Split to Array", description = "SPLIT($columnS, ',')") SPLIT,
        @Label(value = "Repeat String", description = "REPEAT($columnS, 2)") STR_REPEAT,
        @Label(value = "Format Number", description = "FORMAT_NUMBER($columnS, 2)") FORMAT_NUMBER,

        // ── Regular Expression ──
        @Label(value = "Regex Replace (non-digits)", description = "REGEXP_REPLACE($columnS, '[^0-9]', '')") REGEX_NONDIGITS,
        @Label(value = "Regex Extract", description = "REGEXP_EXTRACT($columnS, '(.*)', 1)") REGEX_EXTRACT,
        @Label(value = "Regex Match (RLIKE)", description = "$columnS RLIKE 'pattern'") RLIKE,
        @Label(value = "SQL Pattern (LIKE)", description = "$columnS LIKE '%pattern%'") LIKE,

        // ── Math ──
        @Label(value = "Round to 2 decimals", description = "ROUND($columnS, 2)") ROUND_2,
        @Label(value = "Absolute Value", description = "ABS($columnS)") ABS,
        @Label(value = "Ceiling", description = "CEIL($columnS)") CEIL,
        @Label(value = "Floor", description = "FLOOR($columnS)") FLOOR,
        @Label(value = "Modulo", description = "MOD($columnS, 2)") MODULO,
        @Label(value = "Square Root", description = "SQRT($columnS)") SQRT,
        @Label(value = "Power", description = "POW($columnS, 2)") POW,
        @Label(value = "Sign (+1/0/-1)", description = "SIGN($columnS)") SIGN,
        @Label(value = "Log Base 10", description = "LOG10($columnS)") LOG10,
        @Label(value = "Natural Log (ln)", description = "LN($columnS)") LN,
        @Label(value = "Exponential (e^x)", description = "EXP($columnS)") MATH_EXP,
        @Label(value = "Greatest (max of values)", description = "GREATEST($columnS, 0)") GREATEST,
        @Label(value = "Least (min of values)", description = "LEAST($columnS, 0)") LEAST,

        // ── Date/Time ──
        @Label(value = "Parse Date (yyyyMMdd)", description = "TO_DATE(string($columnS), 'yyyyMMdd')") PARSE_DATE,
        @Label(value = "Parse Timestamp", description = "TO_TIMESTAMP(string($columnS), 'yyyy-MM-dd HH:mm:ss')") PARSE_TIMESTAMP,
        @Label(value = "Format Date/Time", description = "DATE_FORMAT($columnS, 'yyyy-MM-dd')") FORMAT_DATE,
        @Label(value = "Days Difference from Today", description = "DATEDIFF(CURRENT_DATE(), $columnS)") DATEDIFF,
        @Label(value = "Add Days", description = "DATE_ADD($columnS, 1)") DATE_ADD,
        @Label(value = "Subtract Days", description = "DATE_SUB($columnS, 1)") DATE_SUB,
        @Label(value = "Add Months", description = "ADD_MONTHS($columnS, 1)") ADD_MONTHS,
        @Label(value = "Months Between", description = "MONTHS_BETWEEN(CURRENT_DATE(), $columnS)") MONTHS_BETWEEN,
        @Label(value = "Last Day of Month", description = "LAST_DAY($columnS)") LAST_DAY,
        @Label(value = "Truncate to Month", description = "DATE_TRUNC('month', $columnS)") DATE_TRUNC,
        @Label(value = "Extract Year", description = "YEAR($columnS)") EXTRACT_YEAR,
        @Label(value = "Extract Month", description = "MONTH($columnS)") EXTRACT_MONTH,
        @Label(value = "Extract Quarter", description = "QUARTER($columnS)") EXTRACT_QUARTER,
        @Label(value = "Extract Day of Month", description = "DAYOFMONTH($columnS)") EXTRACT_DAYOFMONTH,
        @Label(value = "Extract Day of Week", description = "DAYOFWEEK($columnS)") EXTRACT_DAYOFWEEK,
        @Label(value = "Extract Day of Year", description = "DAYOFYEAR($columnS)") EXTRACT_DAYOFYEAR,
        @Label(value = "Extract Week of Year", description = "WEEKOFYEAR($columnS)") EXTRACT_WEEKOFYEAR,
        @Label(value = "Extract Hour", description = "HOUR($columnS)") EXTRACT_HOUR,
        @Label(value = "Extract Minute", description = "MINUTE($columnS)") EXTRACT_MINUTE,
        @Label(value = "Extract Second", description = "SECOND($columnS)") EXTRACT_SECOND,
        @Label(value = "To Unix Timestamp", description = "UNIX_TIMESTAMP($columnS)") TO_UNIX_TS,
        @Label(value = "From Unix Timestamp", description = "FROM_UNIXTIME($columnS, 'yyyy-MM-dd HH:mm:ss')") FROM_UNIX_TS,

        // ── Null Handling ──
        @Label(value = "Replace NULL with 0", description = "COALESCE($columnS, 0)") NULL_TO_ZERO,
        @Label(value = "Replace NULL with empty string", description = "COALESCE($columnS, '')") NULL_TO_EMPTY,
        @Label(value = "IFNULL (default if null)", description = "IFNULL($columnS, '')") IFNULL,
        @Label(value = "NVL (default if null)", description = "NVL($columnS, '')") NVL,
        @Label(value = "NVL2 (by null status)", description = "NVL2($columnS, 'has value', 'null')") NVL2,
        @Label(value = "NULLIF (null if equal)", description = "NULLIF($columnS, '')") NULLIF,
        @Label(value = "Is Null check", description = "ISNULL($columnS)") IS_NULL,
        @Label(value = "Is Not Null check", description = "ISNOTNULL($columnS)") IS_NOT_NULL,
        @Label(value = "Is NaN check", description = "ISNAN($columnS)") IS_NAN,
        @Label(value = "Replace NaN with default", description = "NANVL($columnS, 0)") NANVL,

        // ── Type Cast ──
        @Label(value = "Cast to String", description = "string($columnS)") CAST_STRING,
        @Label(value = "Cast to Integer", description = "CAST($columnS AS INT)") CAST_INT,
        @Label(value = "Cast to Double", description = "CAST($columnS AS DOUBLE)") CAST_DOUBLE,
        @Label(value = "Cast to BigInt (Long)", description = "CAST($columnS AS BIGINT)") CAST_BIGINT,
        @Label(value = "Cast to Float", description = "CAST($columnS AS FLOAT)") CAST_FLOAT,
        @Label(value = "Cast to Boolean", description = "CAST($columnS AS BOOLEAN)") CAST_BOOLEAN,
        @Label(value = "Cast to Decimal(10,2)", description = "CAST($columnS AS DECIMAL(10,2))") CAST_DECIMAL,
        @Label(value = "Cast to Date", description = "CAST($columnS AS DATE)") CAST_DATE,
        @Label(value = "Cast to Timestamp", description = "CAST($columnS AS TIMESTAMP)") CAST_TIMESTAMP,

        // ── Conditional ──
        @Label(value = "IF condition", description = "IF($columnS > 0, 'positive', 'non-positive')") IF_COND,
        @Label(value = "CASE WHEN", description = "CASE WHEN $columnS IS NULL THEN 'null' ELSE $columnS END") CASE_WHEN,
        @Label(value = "Type Of", description = "TYPEOF($columnS)") TYPEOF,

        // ── Hash / Encoding ──
        @Label(value = "MD5 Hash", description = "MD5(string($columnS))") MD5_HASH,
        @Label(value = "SHA-256 Hash", description = "SHA2(string($columnS), 256)") SHA256_HASH,
        @Label(value = "Base64 Encode", description = "BASE64($columnS)") BASE64_ENCODE,
        @Label(value = "Base64 Decode", description = "UNBASE64($columnS)") BASE64_DECODE,

        // ── JSON ──
        @Label(value = "Get JSON Value", description = "GET_JSON_OBJECT($columnS, '$.key')") JSON_GET,
        @Label(value = "To JSON String", description = "TO_JSON($columnS)") TO_JSON,
        @Label(value = "JSON Array Length", description = "JSON_ARRAY_LENGTH($columnS)") JSON_ARR_LEN,

        // ── Array ──
        @Label(value = "Array Contains", description = "ARRAY_CONTAINS($columnS, 'value')") ARR_CONTAINS,
        @Label(value = "Array Size", description = "ARRAY_SIZE($columnS)") ARR_SIZE,
        @Label(value = "Array Element At", description = "ELEMENT_AT($columnS, 1)") ARR_ELEMENT_AT;

        String getSql() {
            final String ph = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;
            switch (this) {
                // String
                case UPPERCASE:       return "UPPER(" + ph + ")";
                case LOWERCASE:       return "LOWER(" + ph + ")";
                case TRIM:            return "TRIM(" + ph + ")";
                case LTRIM:           return "LTRIM(" + ph + ")";
                case RTRIM:           return "RTRIM(" + ph + ")";
                case STR_LENGTH:      return "LENGTH(" + ph + ")";
                case INITCAP:         return "INITCAP(" + ph + ")";
                case REVERSE:         return "REVERSE(" + ph + ")";
                case CONCAT:          return "CONCAT(" + ph + ", '')";
                case CONCAT_WS:       return "CONCAT_WS(',', " + ph + ", '')";
                case SUBSTRING:       return "SUBSTRING(" + ph + ", 1, 5)";
                case SUBSTRING_INDEX: return "SUBSTRING_INDEX(" + ph + ", ',', 1)";
                case STR_REPLACE:     return "REPLACE(" + ph + ", '', '')";
                case TRANSLATE:       return "TRANSLATE(" + ph + ", 'abc', 'xyz')";
                case LPAD:            return "LPAD(" + ph + ", 10, '0')";
                case RPAD:            return "RPAD(" + ph + ", 10, ' ')";
                case INSTR:           return "INSTR(" + ph + ", '')";
                case SPLIT:           return "SPLIT(" + ph + ", ',')";
                case STR_REPEAT:      return "REPEAT(" + ph + ", 2)";
                case FORMAT_NUMBER:   return "FORMAT_NUMBER(" + ph + ", 2)";
                // Regex
                case REGEX_NONDIGITS: return "REGEXP_REPLACE(" + ph + ", '[^0-9]', '')";
                case REGEX_EXTRACT:   return "REGEXP_EXTRACT(" + ph + ", '(.*)', 1)";
                case RLIKE:           return ph + " RLIKE ''";
                case LIKE:            return ph + " LIKE '%'";
                // Math
                case ROUND_2:         return "ROUND(" + ph + ", 2)";
                case ABS:             return "ABS(" + ph + ")";
                case CEIL:            return "CEIL(" + ph + ")";
                case FLOOR:           return "FLOOR(" + ph + ")";
                case MODULO:          return "MOD(" + ph + ", 2)";
                case SQRT:            return "SQRT(" + ph + ")";
                case POW:             return "POW(" + ph + ", 2)";
                case SIGN:            return "SIGN(" + ph + ")";
                case LOG10:           return "LOG10(" + ph + ")";
                case LN:              return "LN(" + ph + ")";
                case MATH_EXP:        return "EXP(" + ph + ")";
                case GREATEST:        return "GREATEST(" + ph + ", 0)";
                case LEAST:           return "LEAST(" + ph + ", 0)";
                // Date/Time
                case PARSE_DATE:      return "TO_DATE(string(" + ph + "), 'yyyyMMdd')";
                case PARSE_TIMESTAMP: return "TO_TIMESTAMP(string(" + ph + "), 'yyyy-MM-dd HH:mm:ss')";
                case FORMAT_DATE:     return "DATE_FORMAT(" + ph + ", 'yyyy-MM-dd')";
                case DATEDIFF:        return "DATEDIFF(CURRENT_DATE(), " + ph + ")";
                case DATE_ADD:        return "DATE_ADD(" + ph + ", 1)";
                case DATE_SUB:        return "DATE_SUB(" + ph + ", 1)";
                case ADD_MONTHS:      return "ADD_MONTHS(" + ph + ", 1)";
                case MONTHS_BETWEEN:  return "MONTHS_BETWEEN(CURRENT_DATE(), " + ph + ")";
                case LAST_DAY:        return "LAST_DAY(" + ph + ")";
                case DATE_TRUNC:      return "DATE_TRUNC('month', " + ph + ")";
                case EXTRACT_YEAR:         return "YEAR(" + ph + ")";
                case EXTRACT_MONTH:        return "MONTH(" + ph + ")";
                case EXTRACT_QUARTER:      return "QUARTER(" + ph + ")";
                case EXTRACT_DAYOFMONTH:   return "DAYOFMONTH(" + ph + ")";
                case EXTRACT_DAYOFWEEK:    return "DAYOFWEEK(" + ph + ")";
                case EXTRACT_DAYOFYEAR:    return "DAYOFYEAR(" + ph + ")";
                case EXTRACT_WEEKOFYEAR:   return "WEEKOFYEAR(" + ph + ")";
                case EXTRACT_HOUR:         return "HOUR(" + ph + ")";
                case EXTRACT_MINUTE:       return "MINUTE(" + ph + ")";
                case EXTRACT_SECOND:       return "SECOND(" + ph + ")";
                case TO_UNIX_TS:      return "UNIX_TIMESTAMP(" + ph + ")";
                case FROM_UNIX_TS:    return "FROM_UNIXTIME(" + ph + ", 'yyyy-MM-dd HH:mm:ss')";
                // Null Handling
                case NULL_TO_ZERO:    return "COALESCE(" + ph + ", 0)";
                case NULL_TO_EMPTY:   return "COALESCE(" + ph + ", '')";
                case IFNULL:          return "IFNULL(" + ph + ", '')";
                case NVL:             return "NVL(" + ph + ", '')";
                case NVL2:            return "NVL2(" + ph + ", 'has value', 'null')";
                case NULLIF:          return "NULLIF(" + ph + ", '')";
                case IS_NULL:         return "ISNULL(" + ph + ")";
                case IS_NOT_NULL:     return "ISNOTNULL(" + ph + ")";
                case IS_NAN:          return "ISNAN(" + ph + ")";
                case NANVL:           return "NANVL(" + ph + ", 0)";
                // Type Cast
                case CAST_STRING:     return "string(" + ph + ")";
                case CAST_INT:        return "CAST(" + ph + " AS INT)";
                case CAST_DOUBLE:     return "CAST(" + ph + " AS DOUBLE)";
                case CAST_BIGINT:     return "CAST(" + ph + " AS BIGINT)";
                case CAST_FLOAT:      return "CAST(" + ph + " AS FLOAT)";
                case CAST_BOOLEAN:    return "CAST(" + ph + " AS BOOLEAN)";
                case CAST_DECIMAL:    return "CAST(" + ph + " AS DECIMAL(10,2))";
                case CAST_DATE:       return "CAST(" + ph + " AS DATE)";
                case CAST_TIMESTAMP:  return "CAST(" + ph + " AS TIMESTAMP)";
                // Conditional
                case IF_COND:         return "IF(" + ph + " > 0, 'positive', 'non-positive')";
                case CASE_WHEN:       return "CASE WHEN " + ph + " IS NULL THEN 'null' ELSE " + ph + " END";
                case TYPEOF:          return "TYPEOF(" + ph + ")";
                // Hash / Encoding
                case MD5_HASH:        return "MD5(string(" + ph + "))";
                case SHA256_HASH:     return "SHA2(string(" + ph + "), 256)";
                case BASE64_ENCODE:   return "BASE64(" + ph + ")";
                case BASE64_DECODE:   return "UNBASE64(" + ph + ")";
                // JSON
                case JSON_GET:        return "GET_JSON_OBJECT(" + ph + ", '$.key')";
                case TO_JSON:         return "TO_JSON(" + ph + ")";
                case JSON_ARR_LEN:    return "JSON_ARRAY_LENGTH(" + ph + ")";
                // Array
                case ARR_CONTAINS:    return "ARRAY_CONTAINS(" + ph + ", 'value')";
                case ARR_SIZE:        return "ARRAY_SIZE(" + ph + ")";
                case ARR_ELEMENT_AT:  return "ELEMENT_AT(" + ph + ", 1)";
                // Custom
                case CUSTOM:
                default:              return "";
            }
        }
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface TargetColumnsRef      extends ParameterReference<ColumnFilter> {}
    interface TemplateRef           extends ParameterReference<ExpressionTemplate> {}
    interface SqlExpressionRef      extends ParameterReference<String> {}
    interface KeepOriginalRef       extends ParameterReference<Boolean> {}
    interface OutputPatternRef      extends ParameterReference<String> {}
    interface FlowVarSelectorRef    extends ParameterReference<String> {}

    interface CheckButtonRef        extends ButtonReference {}
    interface InsertFlowVarButtonRef extends ButtonReference {}

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

    /** Provides all available flow variables for the dropdown. */
    static final class AllFlowVarsProvider implements FlowVariableChoicesProvider {
        @Override
        public List<FlowVariable> flowVariableChoices(final NodeParametersInput context) {
            return ((NodeParametersInputImpl) context)
                .getAvailableInputFlowVariables(
                    VariableTypeRegistry.getInstance().getAllTypes())
                .values().stream().collect(Collectors.toList());
        }
    }

    // ── PERSISTORS ────────────────────────────────────────────────────────────

    static final class TargetColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkMultiQuerySettings.CFG_TARGET_COLUMNS;

        TargetColumnsPersistor() { super(KEY); }

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

    /** Ephemeral persistor for the template dropdown — always resets to CUSTOM on dialog open. */
    static final class EphemeralTemplatePersistor implements NodeParametersPersistor<ExpressionTemplate> {
        @Override public ExpressionTemplate load(final NodeSettingsRO s) { return ExpressionTemplate.CUSTOM; }
        @Override public void save(final ExpressionTemplate o, final NodeSettingsWO s) {}
        @Override public String[][] getConfigPaths() { return new String[0][]; }
    }

    /** Ephemeral persistor for the flow variable selector — always resets to empty on dialog open. */
    static final class EphemeralStringPersistor implements NodeParametersPersistor<String> {
        @Override public String load(final NodeSettingsRO s) { return ""; }
        @Override public void save(final String o, final NodeSettingsWO s) {}
        @Override public String[][] getConfigPaths() { return new String[0][]; }
    }

    // ── STATE PROVIDERS ───────────────────────────────────────────────────────

    /**
     * Updates the SQL expression field:
     * - When a non-CUSTOM template is selected → replaces SQL with template SQL.
     * - When the Insert button is clicked AND template is (Custom) → appends $$varName.
     *
     * Note: if template is non-CUSTOM, it takes priority over the Insert button.
     * Switch to (Custom) before inserting flow variables.
     */
    static final class SqlExpressionValueProvider implements StateProvider<String> {

        private Supplier<ExpressionTemplate> m_templateSupplier;
        private Supplier<String>             m_flowVarSupplier;
        private Supplier<String>             m_currentSqlSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_templateSupplier   = initializer.computeFromValueSupplier(TemplateRef.class);
            initializer.computeOnButtonClick(InsertFlowVarButtonRef.class);
            m_flowVarSupplier    = initializer.getValueSupplier(FlowVarSelectorRef.class);
            m_currentSqlSupplier = initializer.getValueSupplier(SqlExpressionRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput context) {
            final ExpressionTemplate t = m_templateSupplier.get();
            final String flowVar = m_flowVarSupplier.get();
            String currentSql = m_currentSqlSupplier.get();
            if (currentSql == null) currentSql = "";

            // Non-CUSTOM template always takes priority
            if (t != null && t != ExpressionTemplate.CUSTOM) {
                return t.getSql();
            }

            // CUSTOM + non-empty flow variable: Insert button was clicked
            if (flowVar != null && !flowVar.trim().isEmpty()) {
                return currentSql + "$$" + flowVar.trim();
            }

            return currentSql;
        }
    }

    /**
     * Live SQL preview — updates whenever columns, expression, or options change.
     */
    static final class SqlPreviewProvider implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<ColumnFilter> m_colSupplier;
        private Supplier<String>       m_sqlSupplier;
        private Supplier<Boolean>      m_keepOrigSupplier;
        private Supplier<String>       m_patternSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_colSupplier      = initializer.computeFromValueSupplier(TargetColumnsRef.class);
            m_sqlSupplier      = initializer.computeFromValueSupplier(SqlExpressionRef.class);
            m_keepOrigSupplier = initializer.computeFromValueSupplier(KeepOriginalRef.class);
            m_patternSupplier  = initializer.computeFromValueSupplier(OutputPatternRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            final String[] cols = getManuallySelected(m_colSupplier.get());
            final String expr = m_sqlSupplier.get();
            final boolean keepOrig = Boolean.TRUE.equals(m_keepOrigSupplier.get());
            final String pattern = m_patternSupplier.get();
            final String ph = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;

            if (cols.length == 0) {
                return Optional.of(new TextMessage.Message(
                    "Select target columns in the 'Column Selection' section.",
                    "", TextMessage.MessageType.INFO));
            }
            if (expr == null || expr.trim().isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Enter a SQL expression.", "", TextMessage.MessageType.INFO));
            }
            if (!expr.contains(ph)) {
                return Optional.of(new TextMessage.Message(
                    "SQL expression must contain '" + ph + "' as placeholder.",
                    "", TextMessage.MessageType.WARNING));
            }

            // Collect all existing column names for dedup (preview uses target cols as proxy)
            final java.util.Set<String> usedNames = new java.util.LinkedHashSet<>();
            for (final String c : cols) {
                usedNames.add(c);
            }

            final StringBuilder sb = new StringBuilder("SELECT ");
            for (int i = 0; i < cols.length; i++) {
                if (i > 0) sb.append(", ");
                final String col = cols[i];
                if (keepOrig) sb.append("`").append(col).append("`, ");
                final String resolved = expr.replace(ph, "`" + col + "`");
                String alias = (pattern != null && !pattern.trim().isEmpty())
                    ? pattern.replace(ph, col) : col;
                // Auto-dedup: if keepOrig and alias == original col name, append _1, _2, ...
                if (keepOrig && usedNames.contains(alias)) {
                    int suffix = 1;
                    while (usedNames.contains(alias + "_" + suffix)) {
                        suffix++;
                    }
                    alias = alias + "_" + suffix;
                }
                usedNames.add(alias);
                sb.append(resolved).append(" AS `").append(alias).append("`");
            }
            if (cols.length < 3) sb.append(", ...");
            sb.append(" FROM input");

            return Optional.of(new TextMessage.Message(
                sb.toString(), "", TextMessage.MessageType.INFO));
        }
    }

    /**
     * Runs the Spark validation job on button click.
     * Tests all columns at once; on failure re-tests per-column to identify which failed.
     */
    static final class ValidationProvider implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<ColumnFilter> m_colSupplier;
        private Supplier<String>       m_sqlSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeOnButtonClick(CheckButtonRef.class);
            m_colSupplier = initializer.getValueSupplier(TargetColumnsRef.class);
            m_sqlSupplier = initializer.getValueSupplier(SqlExpressionRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            final Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (!portObjOpt.isPresent()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable validation.",
                    "", TextMessage.MessageType.INFO));
            }

            final String[] cols = getManuallySelected(m_colSupplier.get());
            if (cols.length == 0) {
                return Optional.of(new TextMessage.Message(
                    "Select at least one target column to run validation.",
                    "", TextMessage.MessageType.WARNING));
            }

            final String expr = m_sqlSupplier.get();
            final String ph = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;
            if (expr == null || expr.trim().isEmpty() || !expr.contains(ph)) {
                return Optional.of(new TextMessage.Message(
                    "Enter a valid SQL expression containing the '" + ph + "' placeholder.",
                    "", TextMessage.MessageType.WARNING));
            }

            final SparkDataPortObject sparkPort = (SparkDataPortObject) portObjOpt.get();
            final SparkContextID contextID = sparkPort.getContextID();
            final String inputObjectId = sparkPort.getData().getID();

            try {
                final SparkMultiQueryJobInput jobInput =
                    new SparkMultiQueryJobInput(inputObjectId, cols, expr);
                SparkContextUtil
                    .<SparkMultiQueryJobInput, SparkMultiQueryJobOutput>getJobRunFactory(
                        contextID, SparkMultiQueryNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(contextID);

                return Optional.of(new TextMessage.Message(
                    "OK \u2014 " + cols.length + " column(s) validated successfully.",
                    "", TextMessage.MessageType.SUCCESS));

            } catch (final Exception allEx) {
                final List<String> failedCols = new ArrayList<>();
                final List<String> passedCols = new ArrayList<>();
                String lastError = allEx.getMessage();
                if (allEx.getCause() != null && allEx.getCause().getMessage() != null) {
                    lastError = allEx.getCause().getMessage();
                }
                for (final String col : cols) {
                    try {
                        final SparkMultiQueryJobInput ji =
                            new SparkMultiQueryJobInput(inputObjectId, new String[]{col}, expr);
                        SparkContextUtil
                            .<SparkMultiQueryJobInput, SparkMultiQueryJobOutput>getJobRunFactory(
                                contextID, SparkMultiQueryNodeModel.JOB_ID)
                            .createRun(ji)
                            .run(contextID);
                        passedCols.add(col);
                    } catch (final Exception colEx) {
                        failedCols.add(col);
                    }
                }
                final StringBuilder msg = new StringBuilder("Validation failed.");
                if (!failedCols.isEmpty()) msg.append("\nFailed: ").append(String.join(", ", failedCols));
                if (!passedCols.isEmpty()) msg.append("\nPassed: ").append(String.join(", ", passedCols));
                if (lastError != null) msg.append("\n\nError: ").append(lastError);
                return Optional.of(new TextMessage.Message(
                    msg.toString(), "", TextMessage.MessageType.ERROR));
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Column Selection ──────────────────────────────────────────────────────
    @Layout(DialogSections.ColumnSelectionSection.class)
    @Widget(title = "Target Columns",
        description = "Columns to apply the SQL expression to.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @ValueReference(TargetColumnsRef.class)
    @Persistor(TargetColumnsPersistor.class)
    ColumnFilter m_targetColumns = new ColumnFilter();

    // ── SQL Expression ────────────────────────────────────────────────────────

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "Expression Template",
        description = "Select a preset template to automatically fill the SQL expression field. "
            + "Selecting a non-custom entry replaces the current expression.")
    @Persistor(EphemeralTemplatePersistor.class)
    @ValueReference(TemplateRef.class)
    ExpressionTemplate m_expressionTemplate = ExpressionTemplate.CUSTOM;

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "SQL Expression",
        description = "SQL expression applied to each target column.\n\n"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER
            + " \u2014 placeholder replaced with each target column name.\n\n"
            + "$$variableName \u2014 replaced at execution with the flow variable value. "
            + "STRING variables are automatically single-quoted; INTEGER/DOUBLE are unquoted.\n"
            + "Example: COALESCE(" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + ", $$defaultValue)\n\n"
            + "To insert a flow variable: select from the dropdown below and click Insert.")
    @Persist(configKey = SparkMultiQuerySettings.CFG_SQL_EXPRESSION)
    @ValueReference(SqlExpressionRef.class)
    @ValueProvider(SqlExpressionValueProvider.class)
    String m_sqlExpression = "string(" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + ")";

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "Flow Variable",
        description = "Select a flow variable and click Insert to append $$varName "
            + "at the end of the SQL expression.\n"
            + "STRING variables are single-quoted at execution; INTEGER/DOUBLE are numeric literals.\n"
            + "Note: the Template must be set to '(Custom)' for Insert to take effect.")
    @ChoicesProvider(AllFlowVarsProvider.class)
    @Persistor(EphemeralStringPersistor.class)
    @ValueReference(FlowVarSelectorRef.class)
    String m_flowVarToInsert = "";

    @Layout(DialogSections.ExpressionSection.class)
    @Widget(title = "Insert $$varName",
        description = "Appends $$[variable name] to the SQL expression.")
    @SimpleButtonWidget(ref = InsertFlowVarButtonRef.class, icon = Icon.RELOAD)
    Void m_insertFlowVarButton;

    // ── Output Options ────────────────────────────────────────────────────────
    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Keep original columns",
        description = "Preserve the original target columns and add the transformed columns as new ones. "
            + "Requires the output pattern to differ from '"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + "'.")
    @Persist(configKey = SparkMultiQuerySettings.CFG_KEEP_ORIGINAL)
    @ValueReference(KeepOriginalRef.class)
    boolean m_keepOriginalColumns = false;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Output column pattern",
        description = "Pattern for the output column name. "
            + "Use " + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + " as placeholder.\n"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + " \u2014 replace original column (default)\n"
            + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + "_str \u2014 add new column with suffix")
    @Persist(configKey = SparkMultiQuerySettings.CFG_OUTPUT_PATTERN)
    @ValueReference(OutputPatternRef.class)
    String m_outputColumnPattern = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;

    // ── SQL Preview ───────────────────────────────────────────────────────────
    @Layout(DialogSections.PreviewSection.class)
    @TextMessage(SqlPreviewProvider.class)
    Void m_sqlPreview;

    // ── Validation ────────────────────────────────────────────────────────────
    @Layout(DialogSections.ValidationSection.class)
    @Widget(title = "Run Validation",
        description = "Run a test query (LIMIT 5) to verify the expression against the upstream data. "
            + "Note: $$varName tokens are NOT substituted during dialog validation "
            + "(substitution happens at node execution).")
    @SimpleButtonWidget(ref = CheckButtonRef.class, icon = Icon.RELOAD)
    Void m_checkButton;

    @Layout(DialogSections.ValidationSection.class)
    @TextMessage(ValidationProvider.class)
    Void m_validationDisplay;

    // ── CONSTRUCTOR ───────────────────────────────────────────────────────────

    SparkMultiQueryNodeParameters() {}

    // ── HELPERS ───────────────────────────────────────────────────────────────

    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) return new String[0];
        final ManualFilter mf = filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) return new String[0];
        return mf.m_manuallySelected;
    }
}
