package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple}

import scala.collection.mutable.HashMap

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected(
                              left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                              right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                            ) extends skeleton.Stitch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  private val hashMap: HashMap[Long, Seq[LateTuple]] = HashMap.empty[Long, Seq[LateTuple]]
  private var result: Seq[LateTuple] = Seq.empty[LateTuple]
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    val leftIterator = left.iterator
    while (leftIterator.hasNext) {
      val leftLateTuple : LateTuple = leftIterator.next()
      val key: Long = leftLateTuple.vid
      if (hashMap.contains(key)) {
        hashMap += (key -> (hashMap(key) ++ Seq(leftLateTuple)))
      }
      else {
        hashMap += (key -> Seq(leftLateTuple))
      }
    }

    val rightIterator = right.iterator
    while (rightIterator.hasNext){
      val rightLateTuple : LateTuple = rightIterator.next()
      val key: Long = rightLateTuple.vid
      if (hashMap.contains(key)) {
        val leftLateTuples: Seq[LateTuple] = hashMap.get(key).get
        result = result ++ leftLateTuples.map(x => LateTuple(x.vid, x.value.++(rightLateTuple.value)))
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    if(result.nonEmpty){
      val output = result.head
      //remove first element
      result = result.tail
      return Option(output)
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
