package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */


  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {

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

    //keep only active
    val start = offset.getOrElse(0)
    val end = start + fetch.getOrElse(Int.MaxValue)
    input.execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
      .sortWith{case (t1, t2) => sortByCollation(t1, t2)}
      .slice(start, end)
      .transpose
  }
}

