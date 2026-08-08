# A join's ON clause can address a column it cannot reach

**Status:** open. Root cause located and minimised; one fix attempted and reverted, see below.
Do not re-try that approach.

**Found by:** the FE soak arm (`astfb`, round 114, seed 630365758), signature
`PLAN_INTERNAL_ERROR — plan:StarRocksPlannerException@ExpressionMapping#getColumnRefWithIndex`,
detail `Get columnRef with index 1202 out fieldMappings length`.

**Base:** reproduced on `06d540917ef`.

## Minimal reproduction

160 characters, down from the 18kB mutant:

```sql
create table wide (k1 int, c1 int, ..., c300 int) duplicate key(k1)
    distributed by hash(k1) buckets 1 properties('replication_num'='1');
create table n1 (k1 int, k2 int) ...;   -- deliberately narrow
create table n2 (k1 int, k2 int) ...;

select * from wide where exists (
    select a.k1 from n1 a left join n2 b on a.k1 = b.k1 and c290 = 2);
```

→ `StarRocksPlannerException: Get columnRef with index 294 out fieldMappings length`

`OuterScopeIndexMinimiseProbe` builds this and the variants below.

## Four ingredients, each necessary

| variant | outcome |
| --- | --- |
| unqualified outer column in a join's ON clause, inside EXISTS | **throws** |
| the same with a CTE wrapped around the join | throws (the CTE is incidental) |
| the same column **qualified** (`wide.c290`) | fine |
| the same column in the subquery's **WHERE** instead of the ON clause | fine |
| an **early** outer column (`c1`) instead of a late one | fine — see below |
| outer query projecting narrowly instead of `select *` | fine |
| no subquery at all | fine |

So: the reference must be **unqualified**, it must sit in a **join's ON clause**, the outer relation
must be **wide**, and the column must be **late** in it.

## The half without a crash

The `c1` row above is the dangerous one. The same mistake is made -- an index from the outer scope
is used to address the inner mapping -- but a small index lands *inside* the array, so nothing
throws and the ON clause binds to whichever column happens to occupy that slot. The crash is the
visible half of this defect; a wrong column silently substituted into a join condition is the other.

## Mechanism

`RelationTransformer#visitJoin`, around line 1049:

```java
Scope joinScope = new Scope(RelationId.of(node),
        node.getLeft().getRelationFields().joinWith(node.getRight().getRelationFields()));
joinScope.setParent(node.getScope().getParent());                    // a parent IS set
ExpressionMapping expressionMapping = new ExpressionMapping(joinScope, Streams.concat(
                leftOpt.getFieldMappings().stream(),
                rightOpt.getFieldMappings().stream())
        .collect(Collectors.toList()), generateNewConstMap(...));    // outer fields are NOT appended
```

`ExpressionMapping` addresses fields in one combined space, as its own class comment says: this
relation's fields first, then the enclosing scope's, so "if a child scope has n fields, the first
parent scope field will have index n". Only the constructor that takes an outer `ExpressionMapping`
appends those parent fields.

Here the scope gets a parent but the mapping does not get the parent's fields. The result resolves
outward and cannot address outward: `Scope#resolveField` walks the chain and returns a combined-space
index, `SqlToScalarOperatorTranslator$Visitor#visitSlot:348` hands it to
`ExpressionMapping#getColumnRefWithIndex`, and the array only ever held left + right.

An unqualified name is required because a qualified one binds to a child relation; the ON clause is
required because the WHERE of the subquery is translated against a mapping that does carry the outer
fields.

## Attempted fix, reverted — do not repeat

Passing `outer` so the four-argument constructor appends the parent's fields:

```java
new ExpressionMapping(joinScope, ..., outer, generateNewConstMap(...));
```

It does not work, and it fails in a way worth recording. That constructor also does
`this.scope.setParent(outer.getScope())`, which overwrites the parent set two lines earlier. The
result is not a fix but a different failure: `Column '...tbl_04.col_020' cannot be resolved`, which
proves `outer.getScope()` is **not** `node.getScope().getParent()`.

So `RelationTransformer.outer` is a different level from the parent `visitJoin` installs. That rules
out both halves of the obvious fix: `outer`'s field mappings are the wrong fields to append, and its
scope is the wrong parent to set.

The real fix needs the mapping that corresponds to the parent scope actually installed, which
`visitJoin` does not currently hold -- or an `ExpressionMapping` path that appends fields without
re-parenting, which is only correct once it is established which mapping owns that parent. That is
a question for someone who knows the scope model; guessing a third time in transformer code that
can silently bind the wrong column is not worth it.

## A separate, smaller defect in the same area

```java
public ColumnRefOperator getColumnRefWithIndex(int fieldIndex) {
    if (fieldIndex > fieldMappings.length) {      // should be >=
        throw new StarRocksPlannerException(...);
    }
    return fieldMappings[fieldIndex];             // AIOOBE when fieldIndex == length
}
```

At exactly `length` the guard passes and the array access throws `ArrayIndexOutOfBoundsException`,
discarding the message the guard exists to produce. A negative index is not checked at all.
Independent of the above, safe, and it is what keeps the next occurrence diagnosable.

## Files

- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/RelationTransformer.java:1049-1055` — where the mapping is built
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/ExpressionMapping.java:40-92` — the combined index space, and the constructor that appends
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/SqlToScalarOperatorTranslator.java:343-348` — where it surfaces
- `srfuzz/docs/findings/repro/expressionmapping-index-1202.sql` — the original mutant, replayable with `ExactMutantReplayProbe`
