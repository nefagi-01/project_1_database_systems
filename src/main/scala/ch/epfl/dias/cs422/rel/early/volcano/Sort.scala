package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  /**
    * @inheritdoc
    */
  private var counter = 0
  private var toSort: Seq[Tuple] = Seq.empty[Tuple]
  private var sortedIterator: Iterator[Tuple] = Iterator()

  override def open(): Unit = {
    counter = fetch.getOrElse(-1)
    val inputIterator = input.iterator
    while (inputIterator.hasNext){
      toSort = toSort :+ inputIterator.next()
    }
    val fieldCollations: Array[RelFieldCollation]=collation.getFieldCollations.toArray(Array.ofDim[RelFieldCollation](collation.getFieldCollations.size))
    def sortByCollation(t1: Tuple, t2:Tuple): Boolean = {
      var i = 0
      while (i < fieldCollations.length) {
        val sortInfo = fieldCollations(i)
        val result = t1(sortInfo.getFieldIndex)
          .asInstanceOf[Comparable[Any]].compareTo(
          t2(sortInfo.getFieldIndex)
            .asInstanceOf[Comparable[Any]])

        if (result != 0) {
          if (sortInfo.getDirection.isDescending) {
            if (result > 0)
              return true
            else
              return false
          } else {
            if (result < 0)
              return true
            else
              return false
          }
        }

        i += 1
      }
      true
    }

    toSort=toSort.sortWith{case (t1, t2) => sortByCollation(t1, t2)}

    sortedIterator = toSort.iterator.drop(offset.getOrElse(0))
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (counter != 0 && sortedIterator.hasNext) {
      if (counter > 0) {
        counter = counter - 1
      }
      Some(sortedIterator.next())
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
