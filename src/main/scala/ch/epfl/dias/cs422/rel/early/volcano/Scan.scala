package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store._
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
                       cluster: RelOptCluster,
                       traitSet: RelTraitSet,
                       table: RelOptTable,
                       tableToStore: ScannableTable => Store
                     ) extends skeleton.Scan[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](cluster, traitSet, table)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )
  private var numberRows: Long = 0
  private var currentRow: Int = -1
  private var prog = getRowType.getFieldList.asScala.map(_ => 0)

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    numberRows = scannable.getRowCount
    currentRow= -1

  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    currentRow+=1
    if (currentRow < numberRows) {
      scannable match {
        case store: RowStore =>  return Option(store.getRow(currentRow))
        case _ => return NilTuple
      }
    }
     NilTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = ()
}
