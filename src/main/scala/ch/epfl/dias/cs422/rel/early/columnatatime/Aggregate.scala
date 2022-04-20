package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    //keep only active tuples
    val activeBuffer = input.execute().transpose.filter(row => row.last.asInstanceOf[Boolean])
    if (activeBuffer.isEmpty && groupSet.isEmpty){
      IndexedSeq(aggCalls.map(agg => agg.emptyValue):+ true).transpose.map(column => toHomogeneousColumn(column))
    } else {
      activeBuffer.groupBy(tuple => groupSet.toArray.map(e => tuple(e)).toIndexedSeq).toIndexedSeq.map(toAggregate => toAggregate._1.++(aggCalls.map(agg => toAggregate._2.map(t => agg.getArgument(t)).reduce(agg.reduce))):+true).transpose.map(column => toHomogeneousColumn(column))
    }
  }
}
