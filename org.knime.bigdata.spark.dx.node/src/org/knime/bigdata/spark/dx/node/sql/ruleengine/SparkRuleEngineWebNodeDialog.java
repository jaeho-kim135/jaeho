package org.knime.bigdata.spark.dx.node.sql.ruleengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.webui.data.RpcDataService;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.page.Page;
import org.knime.scripting.editor.GenericInitialDataBuilder;
import org.knime.scripting.editor.ScriptingNodeSettingsService;
import org.knime.scripting.editor.WorkflowControl;

/**
 * WebUI dialog for the Spark Rule Engine node.
 * Provides a modern HTML-based dialog with rule editor, input columns panel,
 * function catalog, and output preview.
 */
@SuppressWarnings("restriction")
final class SparkRuleEngineWebNodeDialog implements NodeDialog {

    private static final String BASE = "/js-src/dist/";

    @Override
    public Page getPage() {
        return Page.create()
            .fromString(() -> SparkRuleEngineWebNodeDialog.class.getResourceAsStream(BASE + "spark-ruleengine.html"))
            .relativePath("spark-ruleengine.html")
            .addResource(() -> SparkRuleEngineWebNodeDialog.class.getResourceAsStream(BASE + "assets/spark-ruleengine.js"), "assets/spark-ruleengine.js")
            .addResource(() -> SparkRuleEngineWebNodeDialog.class.getResourceAsStream(BASE + "assets/spark-ruleengine.css"), "assets/spark-ruleengine.css")
            .addResource(() -> SparkRuleEngineWebNodeDialog.class.getResourceAsStream(BASE + "assets/index.js"), "assets/index.js")
            .addResource(() -> SparkRuleEngineWebNodeDialog.class.getResourceAsStream(BASE + "assets/index.css"), "assets/index.css")
            .addResource(() -> SparkRuleEngineWebNodeDialog.class.getResourceAsStream(BASE + "assets/modulepreload-polyfill.js"), "assets/modulepreload-polyfill.js");
    }

    @Override
    public Set<SettingsType> getSettingsTypes() {
        return Collections.singleton(SettingsType.MODEL);
    }

    @Override
    public NodeSettingsService getNodeSettingsService() {
        final WorkflowControl workflowControl = new WorkflowControl(NodeContext.getContext().getNodeContainer());

        final GenericInitialDataBuilder initialDataBuilder = GenericInitialDataBuilder
            .createDefaultInitialDataBuilder(NodeContext.getContext())
            .addDataSupplier("columnNamesAndTypes", () -> getColumnInfo(workflowControl))
            .addDataSupplier("flowVariables", () -> getFlowVariableInfo(workflowControl))
            .addDataSupplier("functionCatalog", SparkRuleEngineWebNodeDialog::getFunctionCatalog);

        return new ScriptingNodeSettingsService(
            SparkRuleEngineWebSettings::new,
            initialDataBuilder
        );
    }

    @Override
    public Optional<RpcDataService> createRpcDataService() {
        final SparkRuleEngineRpcService rpcService = new SparkRuleEngineRpcService(NodeContext.getContext());

        return Optional.of(RpcDataService.builder()
            .addService("SparkRuleEngineService", rpcService)
            .build());
    }

    @Override
    public boolean canBeEnlarged() {
        return true;
    }

    // ── Initial data suppliers ──────────────────────────────────────────────

    private static Object getColumnInfo(final WorkflowControl workflowControl) {
        try {
            final WorkflowControl.InputPortInfo[] inputInfo = workflowControl.getInputInfo();
            if (inputInfo == null || inputInfo.length == 0) {
                return Collections.emptyList();
            }

            for (final WorkflowControl.InputPortInfo info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final PortObjectSpec portSpec = info.portSpec();
                if (portSpec instanceof SparkDataPortObjectSpec) {
                    return buildColumnList(((SparkDataPortObjectSpec) portSpec).getTableSpec());
                }
            }
            for (final WorkflowControl.InputPortInfo info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final PortObjectSpec portSpec = info.portSpec();
                if (portSpec instanceof DataTableSpec) {
                    return buildColumnList((DataTableSpec) portSpec);
                }
            }
        } catch (final Exception e) {
            // Return empty list on error
        }
        return Collections.emptyList();
    }

    private static List<Map<String, String>> buildColumnList(final DataTableSpec spec) {
        final List<Map<String, String>> columns = new ArrayList<>();
        for (int i = 0; i < spec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = spec.getColumnSpec(i);
            final Map<String, String> col = new LinkedHashMap<>();
            col.put("name", colSpec.getName());
            col.put("type", colSpec.getType().getName());
            columns.add(col);
        }
        return columns;
    }

    @SuppressWarnings("deprecation")
    private static Object getFlowVariableInfo(final WorkflowControl workflowControl) {
        try {
            final FlowObjectStack stack = workflowControl.getFlowObjectStack();
            if (stack == null) {
                return Collections.emptyList();
            }
            final Map<String, FlowVariable> variables = stack.getAllAvailableFlowVariables();
            final List<Map<String, String>> result = new ArrayList<>();
            for (final Map.Entry<String, FlowVariable> entry : variables.entrySet()) {
                final FlowVariable fv = entry.getValue();
                final Map<String, String> varInfo = new LinkedHashMap<>();
                varInfo.put("name", fv.getName());
                varInfo.put("type", fv.getType().name());
                result.add(varInfo);
            }
            return result;
        } catch (final Exception e) {
            return Collections.emptyList();
        }
    }

    // ── Rule Engine Function Catalog ────────────────────────────────────────

    private static List<Map<String, Object>> getFunctionCatalog() {
        final List<Map<String, Object>> catalog = new ArrayList<>();

        // ── Rule Syntax ──────────────────────────────────────────────
        catalog.add(buildCategory("Rule Syntax",
            fn("condition => outcome", "$col$ > 0 => \"positive\"",
                "Basic rule: if condition is true, result is the outcome value."),
            fn("TRUE => outcome (catch-all)", "TRUE => \"default\"",
                "Catch-all rule: always matches. Place at the end as a default."),
            fn("// comment", "// This is a comment",
                "Comment line: ignored during evaluation.")
        ));

        // ── Comparison Operators ─────────────────────────────────────
        catalog.add(buildCategory("Comparison",
            fn("$col$ > value", "$col$ > 0", "Greater than."),
            fn("$col$ < value", "$col$ < 100", "Less than."),
            fn("$col$ >= value", "$col$ >= 0", "Greater than or equal."),
            fn("$col$ <= value", "$col$ <= 100", "Less than or equal."),
            fn("$col$ = value", "$col$ = \"text\"", "Equal to."),
            fn("$col$ != value", "$col$ != 0", "Not equal to.")
        ));

        // ── Logical Operators ────────────────────────────────────────
        catalog.add(buildCategory("Logical",
            fn("cond1 AND cond2", "$col1$ > 0 AND $col2$ < 100",
                "Both conditions must be true."),
            fn("cond1 OR cond2", "$col1$ = \"A\" OR $col1$ = \"B\"",
                "Either condition must be true."),
            fn("NOT cond", "NOT $col$ = 0",
                "Negates the condition."),
            fn("cond1 XOR cond2", "$col1$ > 0 XOR $col2$ > 0",
                "Exactly one condition must be true.")
        ));

        // ── Missing Value ────────────────────────────────────────────
        catalog.add(buildCategory("Missing Value",
            fn("$col$ IS MISSING", "$col$ IS MISSING",
                "True if the column value is null/missing."),
            fn("$col$ IS NOT MISSING", "$col$ IS NOT MISSING",
                "True if the column value is not null/missing.")
        ));

        // ── Pattern Matching ─────────────────────────────────────────
        catalog.add(buildCategory("Pattern Matching",
            fn("$col$ LIKE \"pattern\"", "$col$ LIKE \"%text%\"",
                "SQL LIKE pattern matching. Use % for any chars, _ for single char."),
            fn("$col$ MATCHES \"regex\"", "$col$ MATCHES \"^[A-Z].*\"",
                "Java regex pattern matching."),
            fn("$col$ IN (\"a\", \"b\")", "$col$ IN (\"A\", \"B\", \"C\")",
                "True if the value matches any value in the list.")
        ));

        // ── Outcome Values ───────────────────────────────────────────
        catalog.add(buildCategory("Outcome Values",
            fn("\"string value\"", "\"result text\"",
                "String literal outcome."),
            fn("numeric value", "42",
                "Numeric literal outcome (integer or decimal)."),
            fn("TRUE / FALSE", "TRUE",
                "Boolean literal outcome."),
            fn("MISSING", "MISSING",
                "Null (missing) value outcome."),
            fn("$column$ (column ref)", "$other_col$",
                "Use another column's value as the outcome.")
        ));

        // ── Flow Variables ───────────────────────────────────────────
        catalog.add(buildCategory("Flow Variables",
            fn("$${varName} (auto-detect type)", "$${varName}",
                "Insert a flow variable value. Type is auto-detected at execution time."),
            fn("$${SvarName}$$ (String, legacy)", "$${SvarName}$$",
                "Insert a String flow variable with explicit type prefix (legacy format)."),
            fn("$${IvarName}$$ (Integer, legacy)", "$${IvarName}$$",
                "Insert an Integer flow variable with explicit type prefix (legacy format)."),
            fn("$${DvarName}$$ (Double, legacy)", "$${DvarName}$$",
                "Insert a Double flow variable with explicit type prefix (legacy format).")
        ));

        // ── String Functions (usable in conditions) ──────────────────
        catalog.add(buildCategory("String Functions",
            fn("UPPER(col)", "UPPER()", "Converts to uppercase."),
            fn("LOWER(col)", "LOWER()", "Converts to lowercase."),
            fn("TRIM(col)", "TRIM()", "Removes leading/trailing whitespace."),
            fn("LENGTH(col)", "LENGTH()", "Returns the string length."),
            fn("CONCAT(a, b)", "CONCAT()", "Concatenates strings."),
            fn("SUBSTRING(col, pos, len)", "SUBSTRING()", "Extracts a substring."),
            fn("REPLACE(col, old, new)", "REPLACE()", "Replaces all occurrences."),
            fn("INSTR(col, substr)", "INSTR()", "Position of first occurrence (1-based).")
        ));

        // ── Math Functions ───────────────────────────────────────────
        catalog.add(buildCategory("Math Functions",
            fn("ABS(col)", "ABS()", "Absolute value."),
            fn("ROUND(col, scale)", "ROUND()", "Rounds to specified decimal places."),
            fn("CEIL(col)", "CEIL()", "Rounds up."),
            fn("FLOOR(col)", "FLOOR()", "Rounds down."),
            fn("MOD(a, b)", "MOD()", "Remainder after division."),
            fn("SQRT(col)", "SQRT()", "Square root."),
            fn("POW(col, exp)", "POW()", "Power function.")
        ));

        // ── Date/Time Functions ──────────────────────────────────────
        catalog.add(buildCategory("Date/Time Functions",
            fn("YEAR(col)", "YEAR()", "Extracts the year."),
            fn("MONTH(col)", "MONTH()", "Extracts the month (1-12)."),
            fn("DAYOFMONTH(col)", "DAYOFMONTH()", "Extracts the day (1-31)."),
            fn("HOUR(col)", "HOUR()", "Extracts the hour (0-23)."),
            fn("DATEDIFF(end, start)", "DATEDIFF()", "Days between two dates."),
            fn("DATE_FORMAT(col, fmt)", "DATE_FORMAT()", "Formats a date as string.")
        ));

        // ── Null Handling ────────────────────────────────────────────
        catalog.add(buildCategory("Null Handling",
            fn("COALESCE(a, b)", "COALESCE()", "First non-null value."),
            fn("IFNULL(col, default)", "IFNULL()", "Default if null."),
            fn("ISNULL(col)", "ISNULL()", "True if null."),
            fn("ISNOTNULL(col)", "ISNOTNULL()", "True if not null.")
        ));

        // ── Type Cast ────────────────────────────────────────────────
        catalog.add(buildCategory("Type Cast",
            fn("CAST(col AS type)", "CAST()", "Convert to specified type."),
            fn("STRING(col)", "STRING()", "Cast to string."),
            fn("INT(col)", "INT()", "Cast to integer."),
            fn("DOUBLE(col)", "DOUBLE()", "Cast to double."),
            fn("BOOLEAN(col)", "BOOLEAN()", "Cast to boolean.")
        ));

        // ── Rule Examples ────────────────────────────────────────────
        catalog.add(buildCategory("Rule Examples",
            fn("Age classification",
                "$age$ > 60 => \"Senior\"\n$age$ > 18 => \"Adult\"\nTRUE => \"Minor\"",
                "Classify rows by age into Senior/Adult/Minor."),
            fn("Null handling rule",
                "$status$ IS MISSING => \"Unknown\"\nTRUE => $status$",
                "Replace missing status values with 'Unknown'."),
            fn("Pattern matching",
                "$email$ LIKE \"%@gmail.com\" => \"Gmail\"\n$email$ LIKE \"%@yahoo.com\" => \"Yahoo\"\nTRUE => \"Other\"",
                "Classify email providers using LIKE patterns."),
            fn("Numeric range",
                "$score$ >= 90 => \"A\"\n$score$ >= 80 => \"B\"\n$score$ >= 70 => \"C\"\nTRUE => \"F\"",
                "Assign letter grades based on score ranges."),
            fn("Multi-condition",
                "$dept$ = \"IT\" AND $salary$ > 50000 => \"High-IT\"\n$dept$ = \"IT\" => \"IT\"\nTRUE => \"Other\"",
                "Combine multiple conditions with AND/OR.")
        ));

        return catalog;
    }

    @SafeVarargs
    private static Map<String, Object> buildCategory(final String name,
            final Map<String, Object>... functions) {
        final Map<String, Object> category = new LinkedHashMap<>();
        category.put("name", name);
        category.put("functions", Arrays.asList(functions));
        return category;
    }

    private static Map<String, Object> fn(final String label, final String template, final String description) {
        final Map<String, Object> func = new LinkedHashMap<>();
        func.put("label", label);
        func.put("template", template);
        func.put("description", description);
        return func;
    }
}
