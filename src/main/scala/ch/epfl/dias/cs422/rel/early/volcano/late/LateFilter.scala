package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}
import org.apache.calcite.rex.RexNode
/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateFilter protected (
                            input: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                            condition: RexNode
                          ) extends skeleton.Filter[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](input, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    input.next() match {
      case Some(lateTuple) => if(predicate(lateTuple.value))  Option(lateTuple) else  next()
      case _ =>   NilLateTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
