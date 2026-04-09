package org.knime.bigdata.spark.dx.node;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.dx.node.convert.datetimetostring.SparkDateTimeToStringNodeFactory;
import org.knime.bigdata.spark.dx.node.extract.datetimefields.SparkExtractDateTimeFieldsNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.cellsplit.SparkCellSplitterNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.colcombine.SparkColumnCombinerNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.concatenate.SparkConcatenateNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.constantvalue.SparkConstantValueColumnNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.editcolumn.SparkEditColumnNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.numbertostring.SparkNumberToStringNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.rowsplitter.SparkRowSplitterNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringreplacer.SparkStringReplacerNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringtodatetime.SparkStringToDateTimeNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringtonumber.SparkStringToNumberNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.ruleengine.SparkRuleEngineNodeFactory;

/**
 * Provides DX Spark node factories.
 */
public class DxSparkNodeFactoryProvider extends DefaultSparkNodeFactoryProvider {

    /**
     * Constructor.
     */
    public DxSparkNodeFactoryProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE,
            new SparkUnpivotNodeFactory(),
            new SparkMultiQueryNodeFactory(),
            new SparkExpressionNodeFactory(),
            new SparkStringToNumberNodeFactory(),
            new SparkNumberToStringNodeFactory(),
            new SparkStringToDateTimeNodeFactory(),
            new SparkRuleEngineNodeFactory(),
            new SparkDuplicateRowFilterNodeFactory(),
            new SparkRowSplitterNodeFactory(),
            new SparkDateTimeToStringNodeFactory(),
            new SparkExtractDateTimeFieldsNodeFactory(),
            new SparkStringReplacerNodeFactory(),
            new SparkRankNodeFactory(),
            new SparkConstantValueColumnNodeFactory(),
            new SparkColumnCombinerNodeFactory(),
            new SparkCellSplitterNodeFactory(),
            new SparkEditColumnNodeFactory(),
            new SparkConcatenateNodeFactory());
    }
}
