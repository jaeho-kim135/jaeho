package org.knime.bigdata.spark.dx.node;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.dx.node.calculate.datetimediff.SparkDateTimeDifferenceNodeFactory;
import org.knime.bigdata.spark.dx.node.convert.datetimetostring.SparkDateTimeToStringNodeFactory;
import org.knime.bigdata.spark.dx.node.extract.datetimefields.SparkExtractDateTimeFieldsNodeFactory;
import org.knime.bigdata.spark.dx.node.manipulate.datetimeshift.SparkDateTimeShiftNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.binner.SparkNumericBinnerNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.caseconvert.SparkCaseConvertNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.cellsplit.SparkCellSplitterNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.duplicates.SparkDuplicateRowFilterNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.colcombine.SparkColumnCombinerNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.constantvalue.SparkConstantValueColumnNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.lagcolumn.SparkLagColumnNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.rank.SparkRankNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.rounddouble.SparkRoundDoubleNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.rowsplitter.SparkRowSplitterNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringmanip.SparkStringManipNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringreplacer.SparkStringReplacerNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.topk.SparkTopKRowFilterNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.transpose.SparkTransposeNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.ungroup.SparkUngroupNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.numbertostring.SparkNumberToStringNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringtodatetime.SparkStringToDateTimeNodeFactory;
import org.knime.bigdata.spark.dx.node.preproc.stringtonumber.SparkStringToNumberNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.ruleengine.SparkRuleEngineNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryNodeFactory;

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
            new SparkConstantValueColumnNodeFactory(),
            new SparkRankNodeFactory(),
            new SparkCellSplitterNodeFactory(),
            new SparkColumnCombinerNodeFactory(),
            new SparkStringReplacerNodeFactory(),
            new SparkRowSplitterNodeFactory(),
            new SparkDateTimeToStringNodeFactory(),
            new SparkExtractDateTimeFieldsNodeFactory(),
            new SparkLagColumnNodeFactory(),
            new SparkTopKRowFilterNodeFactory(),
            new SparkUngroupNodeFactory(),
            new SparkStringManipNodeFactory(),
            new SparkTransposeNodeFactory(),
            new SparkNumericBinnerNodeFactory(),
            new SparkRoundDoubleNodeFactory(),
            new SparkDateTimeDifferenceNodeFactory(),
            new SparkDateTimeShiftNodeFactory(),
            new SparkCaseConvertNodeFactory(),
            new SparkDuplicateRowFilterNodeFactory(),
            new SparkRuleEngineNodeFactory(),
            new SparkStringToNumberNodeFactory(),
            new SparkNumberToStringNodeFactory(),
            new SparkStringToDateTimeNodeFactory());
    }
}
