package knime.bigdata.spark_dx.node.unpivot;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentStringListSelection;

/**
 * Minimal dialog:
 *  - Value columns (multi-select)
 *  - Retained columns (multi-select)
 *  - Skip rows containing missing cells
 *
 * Note:
 *  - DialogComponentStringListSelection needs a list of "all columns".
 *  - For now we keep it simple and let user type/select later by flow variable or by future upgrade.
 *  - If you want a perfect KNIME-like column chooser, replace with ColumnFilter2 using input spec.
 */
public final class SparkUnpivotNodeDialog extends DefaultNodeSettingsPane {

    protected SparkUnpivotNodeDialog() {
        final SparkUnpivotSettings s = new SparkUnpivotSettings();

        // Placeholder: empty candidate list for now.
        // Next step: populate candidates from Spark port spec (schema) inside loadAdditionalSettingsFrom().
        addDialogComponent(new DialogComponentStringListSelection(
            s.valueColumnsModel(),
            "Value columns (to unpivot)",
            java.util.Collections.emptyList(),
            true,
            10
        ));

        addDialogComponent(new DialogComponentStringListSelection(
            s.retainedColumnsModel(),
            "Retained columns (keep as-is)",
            java.util.Collections.emptyList(),
            true,
            10
        ));

        addDialogComponent(new DialogComponentBoolean(
            s.skipMissingCellsModel(),
            "Skip rows containing missing cells"
        ));
    }
}
