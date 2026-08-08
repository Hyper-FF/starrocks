# Correlated EXISTS over a UNION plans into a NullPointerException

**Status:** open, root cause located, fix deferred — the choice between rejecting and supporting is
a product decision, see below.

**Found by:** the FE soak arm (`astfb`, round 55, seed 269320929), signature
`PLAN_INTERNAL_ERROR — plan:NullPointerException@java.util.Objects#requireNonNull`.

**Base:** reproduced on `06d540917ef`. The 32 commits between that and `starrocks/main` touch
none of the files involved.

## Minimal reproduction

```sql
create table t  (k1 int, k2 int, k3 varchar(64)) duplicate key(k1) distributed by hash(k1) buckets 1
    properties('replication_num'='1');
create table t2 (k1 int, k2 int, k3 varchar(64)) duplicate key(k1) distributed by hash(k1) buckets 1
    properties('replication_num'='1');
create table t3 (k1 int, k2 int, k3 varchar(64)) duplicate key(k1) distributed by hash(k1) buckets 1
    properties('replication_num'='1');

select k1 from t where exists (
    select k1 from t2 where t2.k1 = 1
    union all
    select k1 from t3 where t3.k2 > t.k2);
```

`NullPointerException`, thrown from `Objects#requireNonNull`, first StarRocks frame
`PruneScanColumnRule#transform:85`.

## What is and is not required

Isolated by running the variants in one pass (`CorrelatedExistsUnionProbe` in the fuzz package,
gated on `-Dsrfuzz.probe`):

| variant | outcome |
| --- | --- |
| correlated EXISTS + UNION + aggregation | NPE |
| correlated EXISTS + UNION, **no aggregation** | NPE |
| correlated EXISTS + UNION, correlation in the FIRST branch | NPE |
| correlated EXISTS + aggregation, **no UNION** | SemanticException (a deliberate rejection) |
| **un**correlated EXISTS + UNION + aggregation | plans fine |

So the two required ingredients are **a correlated EXISTS** and **a UNION in the subquery**.
Aggregation, the `DISTINCT` derived table and the outer GROUP BY that the original mutant carried
are all incidental.

The first reading of the mutation blamed a negative `array_slice` offset -- the edit recorded was
`FunctionCallExpr[1]: 1 -> -1`. Nine variants of that planned cleanly. It was one link of a
three-step chain and the other two mattered; the offset is not involved at all.

## Mechanism

`ExistentialApply2JoinRule.check()` declines to convert an Apply while a correlation column is still
referenced inside the subquery:

```java
return apply.isUseSemiAnti() && apply.isExistential()
        && !SubqueryUtils.containsCorrelationSubquery(input);
```

The intent is that the push-down rules lift the correlated predicate out first. But the push-down
rules cover Filter, Project, AggFilter, AggProjectFilter and Left -- **there is no rule for a set
operation**. So with a UNION in the way the predicate never comes up, the Apply is never converted,
and a column belonging to the OUTER table stays in the predicate of a scan inside a UNION branch.

`PruneScanColumnRule` then trusts that every column reference in a scan's predicate is a column of
that scan:

```java
outputColumns.addAll(Utils.extractColumnRef(scanOperator.getPredicate()));   // line 68
...
Map<ColumnRefOperator, Column> newColumnRefMap = outputColumns.stream()
        .collect(Collectors.toMap(identity(), scanOperator.getColRefToColumnMetaMap()::get));  // line 85
```

`get` returns null for the outer table's column and `Collectors.toMap` rejects a null value with
`Objects.requireNonNull`. The NPE is the symptom; the invalid plan state is the defect.

## Options, and why this is not a one-line fix

**A. Reject cleanly.** Detect a correlated Apply whose inner side is a set operation and throw
`SemanticException`, next to the existing `SubqueryUtils.EXIST_NON_EQ_PREDICATE` and
`NOT_FOUND_CORRELATED_PREDICATE`. Small, consistent with how the sibling unsupported shapes already
behave, and turns an internal NPE into a message a user can act on.

Cost: `EXISTS (... UNION ALL ...)` is legal SQL that other engines support, so this makes a crash
into an explicit refusal rather than making the query work. Both are failures; what changes is what
the user is told. A PR must say so.

**B. Support it.** Add the missing push-down for set operations, distributing the correlated
predicate into each branch. This is an optimizer feature, not a bug fix: branch output alignment,
NOT EXISTS semantics and correlations in more than one branch all need handling.

**C. Not on its own:** filtering `containsKey` in `PruneScanColumnRule`. It stops this NPE and hides
the invalid plan, which will surface somewhere else later with nothing left to say the
decorrelation never happened.

Recommendation: **A** as a `[BugFix]`, **B** separately as a `[Feature]` if the shape is worth
supporting.

## Files

- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/ExistentialApply2JoinRule.java` — the guard
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/SubqueryUtils.java` — `containsCorrelationSubquery`, and where a rejection message would live
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/PruneScanColumnRule.java` — lines 68 and 85, where it surfaces
