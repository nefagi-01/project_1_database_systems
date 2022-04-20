package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.HashMap

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateJoin(
               left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               condition: RexNode
             ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */
  private val leftKeys = getLeftKeys
  private val rightKeys = getRightKeys
  private val hashMap: HashMap[Tuple, Seq[Tuple]] = HashMap.empty[Tuple, Seq[Tuple]]
  private var result: Seq[Tuple] = Seq.empty[Tuple]
  private var i :Long = -1
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    //Build hash table from left
    val leftIterator = left.iterator
    while (leftIterator.hasNext){
      val tuple : Tuple = leftIterator.next().value
      val key = leftKeys.map(el => tuple(el))
      if (hashMap.contains(key)) {
        hashMap += (key -> (hashMap(key) ++ Seq(tuple)))
      }
      else {
        hashMap += (key -> Seq(tuple))
      }
    }

    val rightIterator = right.iterator
    while (rightIterator.hasNext){
      val rightTuple : Tuple = rightIterator.next().value
      val key = rightKeys.map(el => rightTuple(el))
      if (hashMap.contains(key)) {
        val leftTuples: Seq[Tuple] = hashMap.get(key).get
        result = result ++ leftTuples.map(x => x.++(rightTuple))
      }
    }


  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    if(result.nonEmpty) {
      i=i+1
      val output= result.head
      //remove first element
      result = result.tail
      return Option(LateTuple(i,output))
    }
     NilLateTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
