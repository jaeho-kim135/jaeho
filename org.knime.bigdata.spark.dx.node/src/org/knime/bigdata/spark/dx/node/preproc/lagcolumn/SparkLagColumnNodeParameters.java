package org.knime.bigdata.spark.dx.node.preproc.lagcolumn;

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
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.widget.message.TextMessage;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;

/**
 * Node parameters (WebUI dialog settings) for the Spark Lag Column node.
 * Bridges between the WebUI representation and the Settings format used by
 * SparkLagColumnSettings, ensuring compatibility.
 */
@SuppressWarnings("restriction")
class SparkLagColumnNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────

    interface DialogSections {
        @Section(title = "Settings",
            description = "Configure the lag/lead column generation.")
        interface SettingsSection {}

        @Section(title = "Group By",
            description = "Optional group columns for partitioned lag/lead computation.")
        @After(SettingsSection.class)
        interface GroupBySection {}

        @Section(title = "Output",
            description = "Output options.")
        @After(GroupBySection.class)
        interface OutputSection {}

        @Section(title = "Validation")
        @After(OutputSection.class)
        interface ValidationSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum Direction {
        @Label(value = "Lag (previous rows)", description = "Shift values from previous rows.")
        LAG,
        @Label(value = "Lead (next rows)", description = "Shift values from subsequent rows.")
        LEAD;
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface ColumnRef extends ParameterReference<String> {}
    interface OrderColumnRef extends ParameterReference<String> {}
    interface DirectionRef extends ParameterReference<Direction> {}
    interface NumCopiesRef extends ParameterReference<Integer> {}
    interface LagIntervalRef extends ParameterReference<Integer> {}
    interface GroupColumnsRef extends ParameterReference<ColumnFilter> {}
    interface SkipIncompleteRef extends ParameterReference<Boolean> {}

    /** Button reference for the Evaluate button. */
    interface EvaluateButtonRef extends ButtonReference {}

    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

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

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter to/from settings under key "groupColumns".
     */
    static final class GroupColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkLagColumnSettings.CFG_GROUP_COLUMNS;

        GroupColumnsPersistor() {
            super(KEY);
        }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return loadColumnFilterWithFallback(settings, KEY);
        }
    }

    /**
     * Loads a ColumnFilter from settings, trying new NameFilterConfiguration format first
     * and falling back to old SettingsModelFilterString format for backward compatibility.
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
     * Constructs a ColumnFilter with the given selected column names.
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

    /**
     * Bridges Direction enum to/from the string format used by SparkLagColumnSettings.
     * Settings stores "LAG"/"LEAD"; enum names match directly.
     */
    static final class DirectionPersistor implements NodeParametersPersistor<Direction> {
        private static final String CFG_KEY = SparkLagColumnSettings.CFG_DIRECTION;

        @Override
        public Direction load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final String val = settings.getString(CFG_KEY, SparkLagColumnSettings.DIR_LAG);
            if (SparkLagColumnSettings.DIR_LEAD.equals(val)) {
                return Direction.LEAD;
            }
            return Direction.LAG;
        }

        @Override
        public void save(final Direction obj, final NodeSettingsWO settings) {
            final Direction dir = obj != null ? obj : Direction.LAG;
            final String val;
            switch (dir) {
                case LEAD:
                    val = SparkLagColumnSettings.DIR_LEAD;
                    break;
                case LAG:
                default:
                    val = SparkLagColumnSettings.DIR_LAG;
                    break;
            }
            settings.addString(CFG_KEY, val);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    // ── VALIDATION STATE PROVIDER ─────────────────────────────────────────────

    /**
     * Runs a validate-only Spark job when the Evaluate button is clicked.
     */
    static final class EvaluateProvider
        implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<String> m_columnSupplier;
        private Supplier<String> m_orderColumnSupplier;
        private Supplier<Direction> m_directionSupplier;
        private Supplier<Integer> m_numCopiesSupplier;
        private Supplier<Integer> m_lagIntervalSupplier;
        private Supplier<ColumnFilter> m_groupColumnsSupplier;
        private Supplier<Boolean> m_skipIncompleteSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeOnButtonClick(EvaluateButtonRef.class);
            m_columnSupplier = initializer.getValueSupplier(ColumnRef.class);
            m_orderColumnSupplier = initializer.getValueSupplier(OrderColumnRef.class);
            m_directionSupplier = initializer.getValueSupplier(DirectionRef.class);
            m_numCopiesSupplier = initializer.getValueSupplier(NumCopiesRef.class);
            m_lagIntervalSupplier = initializer.getValueSupplier(LagIntervalRef.class);
            m_groupColumnsSupplier = initializer.getValueSupplier(GroupColumnsRef.class);
            m_skipIncompleteSupplier = initializer.getValueSupplier(SkipIncompleteRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            final Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (!portObjOpt.isPresent()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable validation.",
                    "", TextMessage.MessageType.INFO));
            }

            final String column = m_columnSupplier.get();
            if (column == null || column.isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Select a target column to run validation.",
                    "", TextMessage.MessageType.WARNING));
            }

            final String orderColumn = m_orderColumnSupplier.get();
            if (orderColumn == null || orderColumn.isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Select an order-by column to run validation.",
                    "", TextMessage.MessageType.WARNING));
            }

            final SparkDataPortObject sparkPort = (SparkDataPortObject) portObjOpt.get();
            final SparkContextID contextID = sparkPort.getContextID();
            final String inputObjectId = sparkPort.getData().getID();

            final ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

                final Direction dir = m_directionSupplier.get();
                final String dirStr = (dir == Direction.LEAD) ? SparkLagColumnSettings.DIR_LEAD
                                                              : SparkLagColumnSettings.DIR_LAG;
                final int numCopies = m_numCopiesSupplier.get();
                final int lagInterval = m_lagIntervalSupplier.get();
                final String[] groupCols = getManuallySelected(m_groupColumnsSupplier.get());
                final boolean skipIncomplete = Boolean.TRUE.equals(m_skipIncompleteSupplier.get());

                final SparkLagColumnJobInput jobInput = new SparkLagColumnJobInput(
                    inputObjectId,
                    column, orderColumn, dirStr,
                    numCopies, lagInterval, groupCols,
                    skipIncomplete);

                final SparkLagColumnJobOutput output = SparkContextUtil
                    .<SparkLagColumnJobInput, SparkLagColumnJobOutput>getJobRunFactory(
                        contextID, SparkLagColumnNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(contextID);

                final String preview = output.getPreviewData();
                final String msg = (preview != null && !preview.isEmpty())
                    ? "Validation succeeded.\n" + preview
                    : "Validation succeeded.";

                return Optional.of(new TextMessage.Message(
                    msg, "", TextMessage.MessageType.SUCCESS));

            } catch (final Exception e) {
                final String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                return Optional.of(new TextMessage.Message(
                    "Validation failed: " + errMsg,
                    "", TextMessage.MessageType.ERROR));
            } finally {
                Thread.currentThread().setContextClassLoader(originalCL);
            }
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Settings Section ──────────────────────────────────────────────────────

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "Column to lag",
        description = "The column whose values will be shifted (lagged or led).")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Persist(configKey = "column")
    @ValueReference(ColumnRef.class)
    String m_column = "";

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "Order-by column",
        description = "The column used to determine row ordering. Required because Spark DataFrames "
            + "have no inherent row order.")
    @ChoicesProvider(SparkColumnChoicesProvider.class)
    @Persist(configKey = "orderColumn")
    @ValueReference(OrderColumnRef.class)
    String m_orderColumn = "";

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "Direction",
        description = "Whether to shift values from previous rows (Lag) or subsequent rows (Lead).")
    @ValueSwitchWidget
    @Persistor(DirectionPersistor.class)
    @ValueReference(DirectionRef.class)
    Direction m_direction = Direction.LAG;

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "Number of copies (L)",
        description = "Number of lag/lead column copies to create. "
            + "For L copies with interval I, offsets are: I, 2I, 3I, ..., LI.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @Persist(configKey = "numCopies")
    @ValueReference(NumCopiesRef.class)
    int m_numCopies = 1;

    @Layout(DialogSections.SettingsSection.class)
    @Widget(title = "Lag interval (I)",
        description = "The lag interval or periodicity. Defines how many rows to shift per copy.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @Persist(configKey = "lagInterval")
    @ValueReference(LagIntervalRef.class)
    int m_lagInterval = 1;

    // ── Group By Section ──────────────────────────────────────────────────────

    @Layout(DialogSections.GroupBySection.class)
    @Widget(title = "Group columns",
        description = "Optional partition columns. When specified, lag/lead is computed within each group independently.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @ValueReference(GroupColumnsRef.class)
    @Persistor(GroupColumnsPersistor.class)
    ColumnFilter m_groupColumns = new ColumnFilter();

    // ── Output Section ────────────────────────────────────────────────────────

    @Layout(DialogSections.OutputSection.class)
    @Widget(title = "Skip rows with missing lag values",
        description = "If checked, rows where any generated lag/lead column is null will be removed from the output.")
    @Persist(configKey = "skipIncompleteRows")
    @ValueReference(SkipIncompleteRef.class)
    boolean m_skipIncompleteRows = false;

    // ── Validation Section ────────────────────────────────────────────────────

    @Layout(DialogSections.ValidationSection.class)
    @Widget(title = "Evaluate",
        description = "Validate the current configuration against the connected Spark data. "
            + "Requires the upstream node to be executed.")
    @SimpleButtonWidget(ref = EvaluateButtonRef.class, icon = Icon.RELOAD)
    Void m_evaluateButton;

    @Layout(DialogSections.ValidationSection.class)
    @TextMessage(EvaluateProvider.class)
    Void m_validationDisplay;

    // ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    SparkLagColumnNodeParameters() {}

    // ── HELPERS ───────────────────────────────────────────────────────────────

    /**
     * Extracts the manually selected column names from a ColumnFilter.
     */
    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) {
            return new String[0];
        }
        final ManualFilter mf = filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) {
            return new String[0];
        }
        return mf.m_manuallySelected;
    }

    @Override
    public void validate() throws InvalidSettingsException {
        if (m_column == null || m_column.trim().isEmpty()) {
            throw new InvalidSettingsException("No target column selected.");
        }
        if (m_orderColumn == null || m_orderColumn.trim().isEmpty()) {
            throw new InvalidSettingsException("No order-by column selected. "
                + "Spark DataFrames have no inherent row order; an order-by column is required.");
        }
        if (m_numCopies < 1) {
            throw new InvalidSettingsException("Number of copies must be at least 1.");
        }
        if (m_lagInterval < 1) {
            throw new InvalidSettingsException("Lag interval must be at least 1.");
        }
        if ((long) m_numCopies * m_lagInterval > Integer.MAX_VALUE) {
            throw new InvalidSettingsException(
                "The product of number of copies and lag interval exceeds the maximum allowed offset.");
        }
    }
}
