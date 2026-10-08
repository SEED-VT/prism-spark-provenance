# How Prism works

Prism works in two phases. During capture, a single ordinary run of the job writes lineage tables as records flow through it. During a trace, run afterwards and once per suspicious output, Prism joins those tables backwards from the output to the source records. This document explains what each phase records and how the trace reads it.

## Capture with taps

A tap is a pass-through physical operator that Prism inserts into a query's plan. Each tap lets every record through unchanged and, in passing, writes a small lineage table for its partition into Spark's block store on that executor. Prism inserts taps with a Catalyst rule that runs after optimization and before code generation, so the optimizer cannot remove them. The simple taps are fused into Spark's generated code for the stage.

For a job that scans a table, applies a UDF, and aggregates, four taps do the work.

| Tap | Placed | Records |
|---|---|---|
| Scan tap | on the file scan | an identifier for each record, its partition and row offset |
| UDF tap | above the Python UDF | the fields the UDF read for each record identifier |
| Aggregate tap | below the partial aggregate | for each group key hash, the member identifiers and each member's contribution |
| Post tap | above the final aggregate | for each output row, the hash of its group key |

The aggregate tap sits on the map side, before the shuffle folds records together, because that is the last place where each record of a group is still visible on its own. Joins, windows, unions, and `ORDER BY ... LIMIT` get the same pair of taps around their shuffle, keyed on their join or partition keys.

A PySpark UDF runs in a separate Python worker that receives records in batches, which drops the record identifiers. Prism records the identifier sequence below the Python call and replays it above, so each UDF output is matched to the record it came from.

## Field-level lineage through UDFs

Prism rewrites the UDF's source rather than wrapping its values. When the job starts, Prism parses the UDF into its syntax tree on the driver. It replaces every operation with a call to a helper that computes the same value and, in passing, combines the lineage of its operands.

| Source | Rewritten to | Lineage of the result |
|---|---|---|
| `a + b` | `bin('add', a, b)` | the lineage of `a` and of `b` |
| `a < b < c` | `cmp([...], [a, b, c])` | the operands that decided the comparison |
| `a and b` | `bool('and', [λa, λb])` | the operands that were evaluated |
| `a[i]` | `sub(a, i)` | the container, the index, and the element |
| `f(a, b)` | `call(f, [a, b], {})` | the library rule for `f`, or all arguments |
| `x if t else y` | `ifexp(t, λx, λy)` | the chosen branch as data, `t` as control |
| `for x in xs` | `for x in iter(xs)` | the lineage of `xs`, pushed onto each element |

Because the rewrite covers every operation in the tree, no operation can drop lineage, and the values themselves stay plain Python values. A wrapper object that carries provenance inside each value, in contrast, covers only the operators someone implemented for it. A compiled routine such as `numpy.max` rejects such a wrapper because it needs a real number.

Each value carries two kinds of lineage. Data lineage names the fields the value was computed from. Control lineage names the fields that only decided which branch produced it. For example, a UDF that converts a reading to millimetres when its unit is `mm` and otherwise multiplies it by 304.8 reports `value` as data and `unit` as control.

When a UDF calls compiled code, the rewrite sees a single call. A boundary rule states what the function's result depends on. The rule for `max` attributes the result to the largest element alone, and the rule for `sum` attributes it to every element. A call without a rule attributes its result to all of its arguments, which is coarse but never drops a field. Every value records which of these produced it, `exact` for pure Python, `model` for a boundary rule, and `coarse` for an unmodeled call, so a trace reports how precise its answer is. `fineprov.register_model` adds a rule for any function.

Field lineage comes at three granularities, chosen with `enable_udf_aware(mode=...)`.

- **`column`** walks the rewritten tree once on the driver and takes the union over both sides of every branch. It gives one answer for the whole column and adds no per-record cost.
- **`cell`** runs the rewritten UDF on every record and stores a compact code of the fields each record used. It is exact per record and costs the most.
- **`lazy-cell`** runs the job like `column` and computes the per-record split at trace time, over the traced records only. It returns the same answers as `cell` at close to the cost of `column`.

## Influence

The influence of a record on an aggregate output is the amount the output would change if that record were removed. Removing each record and recomputing is the direct way to measure it, but it costs one recomputation per record. For the aggregates Spark runs most often, the same number follows from the record's own contribution and a small summary of its group. The aggregate tap captures both during the job.

| Aggregate | Influence of record *i* with value *x* |
|---|---|
| sum | *x* |
| count | 1 |
| average over *n* records with mean *m* | (*x* - *m*) / (*n* - 1) |
| max | *x* minus the second largest value if *i* holds the maximum, else 0 |
| min | *x* minus the second smallest value if *i* holds the minimum, else 0 |

For a percentile or a median, no small summary suffices. Prism sorts the group's captured values once and reads the percentile with each value left out by skipping its position, which gives the same numbers as recomputing it without each record. A `COUNT(DISTINCT ...)` is planned by Spark as two aggregates, and Prism reports influence at the level of the distinct values. For a percentile, removing a value above the cut point and removing one below it shift the result in opposite directions, so the sign of the influence tells the two groups apart.

`Prism.enable_influence()` turns this on, and `cursor.influence(column)` computes it after a trace. Prism reads which aggregate produced `column` from the query plan, so the caller names a column rather than a formula. In a query with nested aggregations, naming an inner aggregate's column computes influence at that level. For example, with daily totals averaged per store, `influence("store_avg")` ranks a store's days and `influence("daily_total")` ranks the sales within a day.

## Assertion-directed capture

When the suspicious output can be named by its group key before the run, Prism confines capture to that group. `Prism.enable_focus("dep_hr = 23")` passes the predicate to the aggregate tap, which already computes each record's group key and now records lineage only for records that satisfy the predicate. Every other record streams past without being recorded, and the trace over the group is the same as with full capture. A predicate on the aggregate's output, such as `total < 0`, cannot be evaluated before the aggregate exists. Prism then leaves capture unrestricted, so the trace stays complete.

## Tracing

A trace starts from output identifiers and steps backwards one tap at a time. Each step is a join between the identifiers reached so far and the next lineage table. From an output row, the post tap gives its group key hash, and the aggregate tap gives the group's member identifiers. At a join, the trace chooses one of the two inputs with `go_back(branch)`.

Prism runs these joins in one of two ways, chosen by the size of the set reached. A small set, such as a few hundred records, is broadcast into each join and resolved on the driver in seconds. When the set grows past a threshold, the remaining steps run as distributed shuffle joins. The trace's cost therefore follows the number of records it reaches rather than the size of the data.

The last step turns source identifiers back into rows by reading the source table again. Each identifier names the scan partition and the row offset it came from, so Prism reads only the partitions that hold the traced records. The two annotations are added to the rows that come back. Field lineage is decoded from the UDF tap's codes, and influence is computed from the aggregate tap's contributions.

## Limits

Prism covers the physical operators listed in [install.md](install.md#supported-operators) and stops at planning time on any other, rather than return lineage it cannot vouch for. Lineage is stored without replicas in the executors that produced it, so it is lost if an executor is removed. Capture without an assertion grows linearly with the input, like any provenance system that records every output. A UDF can depend on state carried between records, such as a global variable or an external store. No Spark operator sees that dependency, so record lineage does not include it. With two-phase cell capture, a nondeterministic UDF can report a different field split at trace time than it took during the job. Control lineage is tracked through conditional expressions such as `a if t else b`. The per-record tracer does not attribute control lineage to the fields tested by an `if` or `while` statement. Calls into other pure-Python functions are treated like library calls without a rule.
