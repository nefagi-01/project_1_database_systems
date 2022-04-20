# CS422 Project 1  - Stefan Igescu - Relational Operators and Execution Models


## Task 1: Implement a volcano-style tuple-at-a-time engine

The implementation of the six basic operators in a volcano-style tuple-at-a-time has been done in the following way:
### Scan
Scan uses a counter  `currentRow` to scan a tuple at a time until the total number of rows `numberRows` is achieved
### Select
This operator is implemented in the `Filter` class.  Each tuple is taken from `input.next()` and the `match` operator is used to manage the `Option`. The tuple is then filtered using the function `predicate`.
### Project
In order to apply the projection `evaluator` is used within a map `nextTuple.map(evaluator)`.
### Join
The join has been implemented as Hash Join. Because of the volcano-style we can't know the size of left and right table in order to choose the smaller one as build table, therefore I arbitrary choose the left input as build table and the right as probe table. The hash table is implemented with a `HashMap[Tuple, Seq[Tuple]]` and it is populated and probed in the `open()`. The result is stored in a `Seq[Tuple]` and each time `next()` is called a tuple is taken from the head of the sequence and returned.

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
* *Join* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateJoin]) is also an adaptation of the previous version by accessing the field `value`. In addition, a new vid is generated for the resulting `LateTuple` by using the counter `i`.
* *Project* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateProject]) Same as before. It preserves the previous `vid`.


## Task 3: Query Optimization Rules


### Subtask 3.A: Implement the Fetch operator

The implementation of the `Fetch` is done in `next()`:
1. We obtain `nextTuple` from `input.next()`
2. If we obtain a tuple then we access `column` at the position `nextTuple.vid`
3. If we find a corresponding value in `column` we apply the projection if `projects` is not empty.
4. At the end we return a `LateTuple` with the same `vid` and with the tuple containing now also the value from `column`

### Subtask 3.B: Implement the Optimization rules

- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchRule] the implementation is done by accessing the field `rels` of `call: RelOptRuleCall`.
  1. We store the stitch logical operator `stitch = call.rels(0).asInstanceOf[LogicalStitch]`
  2. 
- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchFilterRule] to replace a Stitch &rarr; Filter with a Fetch &rarr; Filter,
- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchProjectRule] to replace a Stitch &rarr; Project with a Fetch.

Example: LazyFetchRule transforms the following subplan

Stitch

&rarr; Filter

&rarr; &rarr; LateColumnScan(A.x)

&rarr; LateColumnScan(A.y)

to

Fetch (A.y)

&rarr; Filter

&rarr; &rarr; LateColumnScan(A.x)

## Task 4: Execution Models (30%)

This tasks focuses on the column-at-a-time execution model, building gradually from an operator-at-a-time execution over
columnar data.

### Subtask 4.A: Enable selection-vectors in operator-at-a-time execution

A fundamental block in implementing vector-at-a-time execution is selection-vectors. In this task you should implement
the

* **Filter** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Filter])
* **Project** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Project])
* **Join** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Join])
* **Scan** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Scan])
* **Sort** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Sort])
* **Aggregate** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Aggregate])

for operator-at-a-time execution over columnar
inputs. Your implementation should be based on selection vectors and (`Tuple=>Tuple`) evaluators. That is, all operators receive one extra column of `Boolean`s (the last column) that signifies
which of the inputs tuples are active. The Filter, Scan, Project should not prune tuples, but only set the selection
vector. For the Join and Aggregate you are free to select whether they only generate active tuples or they also produce
inactive tuples, as long as you conform with the operator interface (extra Boolean column).

### Subtask 4.B: Column-at-a-time with selection vectors and mapping functions

In this task you should implement

* **Filter** ([ch.epfl.dias.cs422.rel.early.columnatatime.Filter])
* **Project** ([ch.epfl.dias.cs422.rel.early.columnatatime.Project])
* **Join** ([ch.epfl.dias.cs422.rel.early.columnatatime.Join])
* **Scan** ([ch.epfl.dias.cs422.rel.early.columnatatime.Scan])
* **Sort** ([ch.epfl.dias.cs422.rel.early.columnatatime.Sort])
* **Aggregate** ([ch.epfl.dias.cs422.rel.early.columnatatime.Aggregate])

for columnar-at-a-time execution over columnar inputs
with selection vectors, but this time instead of using the evaluators that work on tuples (`Tuple => Tuple`), you should
use the `map`-based provided functions that evaluate one expression for the full
input (`Indexed[HomogeneousColumn] => HomogeneousColumn`).

__Hint__: You can convert a `Column` to `HomogeneousColumn` by using `toHomogeneousColumn()`.

## Project setup & grading

### Setup your environment

The skeleton codebase is pre-configured for development in [IntelliJ (version 2020.3+)](https://www.jetbrains.com/idea/) and this is the only supported IDE. You are free to
use any other IDE and/or IntelliJ version, but it will be your sole responsibility to fix any configuration issues you
encounter, including that through other IDEs may not display the provided documentation.

After you install IntelliJ in your machine, from the File menu select
`New->Project from Version Control`. Then on the left-hand side panel pick `Repository URL`. On the right-hand side
pick:

* Version control: Git
* URL: [https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/Project-1-username](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/)
  or [git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2022/students/Project-1-username](git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2022/students/)
  , depending on whether you set up SSH keys (where <username> is your GitLab username).
* Directory: anything you prefer, but in the past we have seen issues with non-ascii code paths (such as french
  punctuations), spaces and symlinks

IntelliJ will clone your repository and setup the project environment. If you are prompt to import or auto-import the
project, please accept. If the JDK is not found, please use IntelliJ's option to `Download JDK`, so that IntelliJ
install the JDK in a location that will not change your system settings and the IDE will automatically configure the
project paths to point to this JDK.

### Personal repository

The provided
repository ([https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/Project-1-username](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/))
is personal and you are free to push code/branches as you wish. The grader will run on all the branches, but for the
final submission only the master branch will be taken into consideration.

### Additional information and documentation

The skeleton code depends on a library we provide to integrate the project with a state-of-the-art query optimizer,
Apache Calcite. Additional information for Calcite can be found in it's official
site [https://calcite.apache.org](https://calcite.apache.org)
and it's documentation site [https://calcite.apache.org/javadocAggregate/](https://calcite.apache.org/javadocAggregate/).

Documentation for the integration functions and helpers we provide as part of the project-to-Calcite integration code
can be found either be browsing the javadoc of the dependency jar (External Libraries/ch.epfl.dias.cs422:base), or by
browsing to
[http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html](http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html)
WHILE ON VPN.

*If while browsing the code IntelliJ shows a block:*

```scala
/**
 * @inheritdoc
 */

```

Next to it, near the column with the file numbers, the latest versions of IntelliJ have a paragraph symbol
to `Toggle Render View` (to Reader Mode) and get IntelliJ to display the properly rendered inherited prettified
documentation.
*In addition to the documentation in inheritdoc, you may want to browse the documentation of parent classes (
including the skeleton operators and the parent Operator and [ch.epfl.dias.cs422.helpers.rel.RelOperator] classes)*

***Documentation of constructor's input arguments and examples are not copied by the IntelliJ's inheritdoc command, so
please visit the parent classes for such details***

### Submissions & deliverables

Submit your code and short report, by pushing it to your personal gitlab project before the deadline. The repositories
will be frozen after the deadline and we are going to run the automated grader on the final tests.

We will grade the last commit on the `master` branch of your GitLab repository. In the context of this project you only
need to modify the ``ch.epfl.dias.cs422.rel'' package. Except from the credential required to get access to the
repository, there is nothing else to submit on moodle for this project. Your repository must contain a `Report.pdf` or
`report.md` which is a short report that gives a small overview of the peculiarities of your implementation and any
additional details/notes you want to submit. If you submit the report in markdown format, you are responsible for making
sure it renders properly on gitlab.epfl.ch's preview.

To evaluate your solution, run your code with the provided tests ([ch.epfl.dias.cs422.QueryTest] class).

#### Grading

Keep in mind that we will test your code automatically.
Any project that fails to conform to the original skeleton code
and interfaces will fail in the auto grader, and hence, will be graded as a zero.
More specifically, you should not change the function and constructor signatures provided in the skeleton code, or make any other change that will break interoperability with the base library.

You are allowed to add new classes, files and packages, but only under the current package. Any code outside the current
package will be ignored and not graded. You are free to edit the `Main.scala` file and/or create new `tests`, but we are
going to ignore such changes during grading.

Tests that timeout will lose all the points for the timed-out test cases, as if they returned wrong results.
