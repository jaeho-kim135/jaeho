package org.knime.bigdata.spark.dx.node.preproc.numbertostring;

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Number to String node.
 */
public final class SparkNumberToStringSettings {

    static final String CFG_INCLUDE = "include";

    private static final String KEY_FILTER_TYPE = "filter-type";
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    private static final String KEY_INCLUDED_NAMES = "included_names";
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    private String[] m_includedColumns = new String[0];

    /** @return the included column names */
    public List<String> getIncludedColumns() {
        return Arrays.asList(m_includedColumns);
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        final NodeSettingsWO sub = settings.addNodeSettings(CFG_INCLUDE);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, m_includedColumns);
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_INCLUDE)) {
            throw new InvalidSettingsException("Missing column filter configuration for key '" + CFG_INCLUDE + "'.");
        }
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(CFG_INCLUDE)) {
            return;
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(CFG_INCLUDE);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                m_includedColumns = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                return;
            }
            m_includedColumns = sub.getStringArray("InclList", new String[0]);
        } catch (final InvalidSettingsException e) {
            m_includedColumns = new String[0];
        }
    }
}
