package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    //keep only active
    val bufferActiveRight = right.execute().transpose
      .filter(row => row.last.asInstanceOf[Boolean])
    val bufferActiveLeft = left.execute().transpose
      .filter(row => row.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))

    val rightKeys = getRightKeys
    val leftKeys = getLeftKeys

    if(bufferActiveLeft.size > bufferActiveRight.size) {
      val mapRight: Map[Tuple, Seq[Tuple]] = bufferActiveRight.groupBy(t => rightKeys.map(e => t(e)))

      bufferActiveLeft.flatMap(t => {
        mapRight.get(leftKeys.map(e => t(e))) match {
          case Some(tuples) => tuples.map(t :++ _)
          case _ => IndexedSeq.empty
        }
      })
        .transpose
        .map(column => toHomogeneousColumn(column))
    }else{
      val mapLeft: Map[Tuple, Seq[Tuple]] = bufferActiveLeft.groupBy(t => leftKeys.map(e => t(e)))
      bufferActiveRight.flatMap(t => {
        mapLeft.get(rightKeys.map(e => t(e))) match {
          case Some(tuples) => tuples.map(_ :++ t)
          case _ => IndexedSeq.empty
        }
      })
        .transpose
        .map(column => toHomogeneousColumn(column))
    }
  }
}
