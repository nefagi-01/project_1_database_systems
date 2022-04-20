package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  private var aggregationMap: Map[Tuple, Seq[Tuple]] = Map.empty[Tuple, Seq[Tuple]]
  private var aggregationSeq: IndexedSeq[(Tuple, Seq[Tuple])] = IndexedSeq.empty[(Tuple, Seq[Tuple])]
  private var aggEmpty = false
  private var result: Tuple = _
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    //open() builds the aggregationMap
    val inputIterator = input.iterator
    if (groupSet.isEmpty && inputIterator.isEmpty) {
      // return aggEmptyValue for each aggregate
      result =  aggCalls.map(agg => agg.emptyValue)
      aggEmpty=true
    } else {
      //build aggregationMap iterating over tuples from input
      while(inputIterator.hasNext) {
          val tuple: Tuple = inputIterator.next()
          val key: Tuple = groupSet.toArray.map(e => tuple(e))
          if (aggregationMap.contains(key)) {
            aggregationMap += (key -> (aggregationMap(key) ++ Seq(tuple)))
          } else {
            aggregationMap += (key -> Seq(tuple))
          }
        }
      aggregationSeq = aggregationMap.toIndexedSeq
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (aggEmpty) {
      aggEmpty = false
      return Option(result)
    }

    if (aggregationSeq.nonEmpty) {
      //aggregate on first element of sequence
      val toAggregate=aggregationSeq(0)
      val result = toAggregate._1.++(aggCalls.map(agg => toAggregate._2.map(t => agg.getArgument(t)).reduce(agg.reduce)))
      //remove first element
      aggregationSeq = aggregationSeq.tail

      Option(result)
    } else {

      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
