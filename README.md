# CS422 Project 1  - Stefan Igescu - Relational Operators and Execution Models


## Task 1: Implement a volcano-style tuple-at-a-time engine

The implementation of the six basic operators in a volcano-style tuple-at-a-time has been done in the following way.
### Scan
Scan uses a counter  `currentRow` to scan a tuple at a time until the total number of rows `numberRows` is achieved. We use the `match` operator to verify if `scannable` is defined and to cast it to `RowStore` in order to be able to use the `.getRow` method to obtain the rows.
### Select
This operator is implemented in the `Filter` class.
We first try to fetch a `Option[Tuple]` using `input.next()`. Afterwards we check if the tuple is defined using the `match` operator. If there is a value inside the `Option` then we filter it using the `predicate` function: if it returns true, then the `Tuple` is returned to the next operator, otherwise we call recursively `next()` until we either find a `Tuple` returning `true`, either we finish the table.

### Project
The way we retrieve and manage `Option[Tuple]` is similar to before. In this case we apply projection using `map` in this way `nextTuple.map(evaluator)`.
### Join
The join has been implemented as Hash Join. Because of the volcano-style we can't know the size of left and right table in advance in order to choose the smaller one as build table, therefore we always choose the left input as build table and the right as probe table. The hash table is implemented with a `HashMap[Tuple, Seq[Tuple]]` and it is populated and probed in the `open()`. The result is stored in a `Seq[Tuple]` and each time `next()` is called a tuple is taken from the head of the sequence and returned.

### Aggregate
The operator is implemented using  `aggregationMap: Map[Tuple, Seq[Tuple]]` where the key consists on the attributes on which the aggregation is done, whereas the value is a `Seq[Tuple]` which collects all the tuples with the respective key. In a second moment the Map is transformed into a `aggregationSeq: IndexedSeq[(Tuple, Seq[Tuple])]` and is then processed each time `next()` is called: 
1. we take a key-value pair from the head of`aggregationSeq`
2. we combine the key with the result of mapping and reducing the `aggCalls` over the tuples associated to that key

### Sort 
In order to sort all the tuples we need at first to store all of them. Indeed, in the function `open()` we:
1. Store all the tuples from the input into `toSort: Seq[Tuple]`
2. Sort them using the function `sortByCollation`
3. We drop the first tuples based on the `offset` value.

Afterwards in the function `next()` we return a tuple at a time by paying attention to return a maximum of `fetch` tuples. This is done by using the variable `counter`.


## Task 2: Late Materialization (naive)


### Subtask 2.A: Implement Late Materialization primitive operators

- Drop ([ch.epfl.dias.cs422.rel.early.volcano.late.Drop]) is implemented by simply accessing to the field `value` of the `LateTuple` and by returning it. 
- Stitch ([ch.epfl.dias.cs422.rel.early.volcano.late.Stitch]) is implemented by using `hashMap: HashMap[Long, Seq[LateTuple]]` where the keys are the `vids` and the values are the tuples associated. We start from the left input, and we build the `hashMap` based on it. Afterwards, for each tuple in the right input we try to find a match in `hashMap`. If this is the case we stitch the tuples and return the final `LateTuple` as a `Option[LateTuple]`

### Subtask 2.B: Extend relational operators to support execution on LateTuple data

* *Filter* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateFilter]) is implementing by adapting the previous version of the Filter by accessing the field `value` of the `LateTuple`. It preserves the previous `vid`.
* *Join* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateJoin]) is also an adaptation of the previous version by accessing the field `value`. In addition, a new vid is generated for the resulting `LateTuple` by using the counter `i` `Option(LateTuple(i,output))`.
* *Project* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateProject]) Same as before. It preserves the previous `vid`.


## Task 3: Query Optimization Rules


### Subtask 3.A: Implement the Fetch operator

The implementation of the `Fetch` is done in `next()`:
1. We obtain `Option[LateTuple]` from `input.next()` and store it in `nextTuple`.
2. If we obtain a `LateTuple` then we access `column` at the position `nextTuple.vid`.
3. If we find a corresponding value in `column` we apply the projection if `projects` is not empty.
4. At the end we return a `LateTuple` with the same `vid` and with the tuple containing now also the value from `column`.

### Subtask 3.B: Implement the Optimization rules

- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchRule] the implementation is done by accessing the field `rels` of `call: RelOptRuleCall`.
  1. We store the input of the stitch operator in `inputStitch = call.rels(1)` by accessing the field `call.rels(1)` at position `1`.
  2. We store the value `columnScan = call.rels(2).asInstanceOf[LateColumnScan]`. From this variable we will get the column input needed in the Fetch operator by accessing it this way `columnScan.getColumn`.
  3. We create the new Fetch logical operator by using the corresponding `create` method `LogicalFetch.create(inputStitch,columnScan.getRowType, columnScan.getColumn, None, classOf[LogicalFetch])`
- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchFilterRule] in this case we return a `LogicalFilter` operator which takes as input a `LogicalFetch` operator. As before we access the logical operators by using the `call.rels` field. In this case we have to pay particular attention to the following things:
  1. The filter condition has to be shifted based on the number of fields we add to the `LateTuple` because of the Fetch. This is done by using the function `RexUtil.shift(filter.getCondition,0,inputStitch.getRowType.getFieldCount)`
  2. The input of the Filter operator is stored in `newFetch = LogicalFetch.create(inputStitch,columnScan.getRowType,columnScan.getColumn,None,classOf[LogicalFetch])`
- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchProjectRule] the procedure is again similar to the previous cases. However, in this case we leverage the field `projects` in the `Fetch` operator in order to apply the projection on the `LateTuple`. It was indeed important to correctly implement the `Fetch` operator in order to correctly apply the projection if the `projects` field is given.

## Task 4: Execution Models

### Subtask 4.A: Enable selection-vectors in operator-at-a-time execution
In all these operators a buffer has been implemented in order to store the vector of tuples which are taken from the input. Afterwards this buffer is processed accordingly to the operator. It was particularly important to use the `.transpose` method in order to process the values as `Tuple`.
* **Filter** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Filter]): in this case the processing consists in only changing the extra flag column containing the `Boolean` values. Here we will change them based on the filter condition. It is important to keep to false the values which were already set to false even if the predicate returns true. That's why we use a `&&` operator between the previous value and the one returned from the `predicate`.  `bufferValues :+ bufferValues.transpose.zipWithIndex.map{case (t,i) => bufferFlag(i).asInstanceOf[Boolean] && predicate(t)}`. After the flag column has been processed then it is again added to the others.
* **Project** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Project]): in this case we don't change the flag column. We only map over the transposed field columns in order to apply projection.
* **Join** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Join]): we have 2 buffers for respectively the left and right inputs. Only the active tuples are processed in this operator. Differently from the volcano case, we have both inputs in the beginning, therefore we can build the hash table on the smaller one. Since we have a vector of tuples we can group them using the `.groupBy`  method without creating the groups manually as done in the volcano case. The matching is done using the `.flatMap` method.
* **Sort** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Sort]): in this operator only active tuples are processed and returned. The sorting is done similarly to the volcano operator. In this case we use the `.slice` operator to remove the not desired tuples according to the `offset` and `fetch` values.
* **Aggregate** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Aggregate]): also in this operator only active tuples are processed and returned. Since we have a vector of tuples we can group them using the `.groupBy`  method without creating the groups manually as done in the volcano case. Again we apply `map` and `reduce` on the grouped `Tuple` based on `aggCall`.

### Subtask 4.B: Column-at-a-time with selection vectors and mapping functions


* **Filter** ([ch.epfl.dias.cs422.rel.early.columnatatime.Filter]) differently from the operator-at-a-time case we process directly the columns without using the `.transpose` method. Again, in the Filter operator we only change the values of the flag column.
* **Project** ([ch.epfl.dias.cs422.rel.early.columnatatime.Project]) also in the project case we directly process the columns without using `.transpose`. 
* **Join** ([ch.epfl.dias.cs422.rel.early.columnatatime.Join]) in this case we have to change the column representation to the tuple one using `.transpose`. After processing the tuples as in the operator-at-a-time case we re-transpose them to column. Finally, we transform all columns to `HomogeneousColumn` using the method `toHomogeneousColumn()`. Again only active tuples are processed.
* **Sort** ([ch.epfl.dias.cs422.rel.early.columnatatime.Sort]) the implementation is similar to the previous case. Again we have to transform all columns to `HomogeneousColumn` using the method `toHomogeneousColumn()`. Again only active tuples are processed.
* **Aggregate** ([ch.epfl.dias.cs422.rel.early.columnatatime.Aggregate])  the implementation is similar to the previous case. Again we have to transform all columns to `HomogeneousColumn` using the method `toHomogeneousColumn()`. Again only active tuples are processed.
