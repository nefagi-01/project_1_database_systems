package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * NOTE: in column-at-a-time execution with selection vectors, the
  * filter does not prune the tuples, only marks the corresponding
  * entries in the selection vector as false.
  * Removing tuples will be penalized.
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val mappredicate: IndexedSeq[HomogeneousColumn] => Array[Boolean] = {
    val evaluator = map(condition, input.getRowType, isFilterCondition = true)
    (t: IndexedSeq[HomogeneousColumn]) => unwrap[Boolean](evaluator(t))
  }

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    val executed = input.execute()
    //use method unwrap from HomogeneousColumn to transform HomogeneousColumn into array
    val lastArray : Array[Boolean] = unwrap[Boolean](executed.last)
    val predicateArray : Array[Boolean]= mappredicate(executed.dropRight(1))
    //transform final flag array again into HomogeneousColumn
    val resultFlagArray : HomogeneousColumn= toHomogeneousColumn(lastArray.zip(predicateArray).map{ case (a,b) => a && b})
    executed.dropRight(1) :+ resultFlagArray

  }
}
