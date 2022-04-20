package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchFilterRuleSkeleton
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rex.RexUtil

/**
  * RelRule (optimization rule) that finds a reconstruct operator that
  * stitches a filtered column (scan then filter) with the late materialized
  * tuple and transforms stitching into a fetch operator followed by a filter.
  *
  * To use this rule: LazyFetchProjectRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchFilterRule protected (config: RelRule.Config)
  extends LazyFetchFilterRuleSkeleton(
    config
  ) {




  override def onMatchHelper(call: RelOptRuleCall): RelNode = {
    val inputStitch = call.rels(1)
    val filter = call.rels(2).asInstanceOf[LogicalFilter]
    val columnScan = call.rels(3).asInstanceOf[LateColumnScan]
    val newFetch = LogicalFetch.create(inputStitch,columnScan.getRowType,columnScan.getColumn,None,classOf[LogicalFetch])
    LogicalFilter.create(newFetch, RexUtil.shift(filter.getCondition,0,inputStitch.getRowType.getFieldCount))
  }

}

object LazyFetchFilterRule {

  /**
    * Instance for a [[LazyFetchFilterRule]]
    */
  val INSTANCE = new LazyFetchFilterRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
      // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalStitch]
        b.operand(classOf[LogicalStitch])
          // that has inputs:
          .inputs(
            b1 =>
              // A node that is a LateColumnScan
              b1.operand(classOf[RelNode])
                // of any inputs
                .anyInputs(),
            b2 =>
              // A node that is a LateColumnScan
              b2.operand(classOf[LogicalFilter])
                // of any inputs
                .oneInput(
                  b3 =>
                    b3.operand(classOf[LateColumnScan])
                      .anyInputs()
                )
          )
      )
  )
}
