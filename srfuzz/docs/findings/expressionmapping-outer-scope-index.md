# A field index from an outer scope is used to index the inner mapping

**Status:** open, mechanism located from the stack, not yet minimised, fix deferred.

**Found by:** the FE soak arm (`astfb`, round 114, seed 630365758), signature
`PLAN_INTERNAL_ERROR — plan:StarRocksPlannerException@ExpressionMapping#getColumnRefWithIndex`,
detail `Get columnRef with index 1202 out fieldMappings length`.

**Base:** reproduced on `06d540917ef`.

## Reproduction

Exact, and it does reproduce:

```
mvn -pl fe-core surefire:test -Dtest=ExactMutantReplayProbe \
    -Dsrfuzz.replay.setup=<corpus>/cluster_b_adhoc_p000.sql \
    -Dsrfuzz.replay.sql=srfuzz/docs/findings/repro/expressionmapping-index-1202.sql
```

The statement is kept verbatim beside this file. It is 18kB because the seed was
`SELECT * FROM tbl_04 WHERE col_020=2 LIMIT 3` over a table with hundreds of columns and the star
is expanded; the mutation was a single `M10-splice exists` that spliced in an EXISTS carrying a
`WITH` clause.

**Not yet minimised.** Seven hand-written shapes around "EXISTS containing a CTE" all plan cleanly,
and the correlated ones are cleanly rejected with `Not support exists correlated...` from
`ExistentialApply2JoinRule`. Whatever the remaining ingredient is, it is not the shape alone -- the
width of the outer table looks load-bearing, see below.

## Mechanism

```java
// SqlToScalarOperatorTranslator$Visitor#visitSlot:345-348
ResolvedField resolvedField =
        expressionMapping.getScope().resolveField(node, expressionMapping.getOuterScopeRelationId());
ColumnRefOperator columnRefOperator =
        expressionMapping.getColumnRefWithIndex(resolvedField.getRelationFieldIndex());
```

`resolveField` may resolve the slot in an **outer** scope -- the next three lines exist precisely to
notice that and record a correlation:

```java
if (!expressionMapping.getScope().isLambdaScope() &&
        resolvedField.getScope().getRelationId().equals(expressionMapping.getOuterScopeRelationId())) {
    correlation.add(columnRefOperator);
}
```

But `getRelationFieldIndex()` is an index into the scope the field resolved in, and it is handed
straight to `getColumnRefWithIndex`, which indexes **this** mapping's `fieldMappings`. Two index
spaces, no translation between them. With a wide outer relation the outer index runs past the inner
mapping's length and the guard fires; 1202 is a column count, not an off-by-one.

The dangerous corollary: when the outer index happens to be **within** the inner mapping's length,
nothing fires and a silently wrong ColumnRefOperator is returned. A crash is the visible half of
this defect.

## A separate, smaller defect in the same method

```java
public ColumnRefOperator getColumnRefWithIndex(int fieldIndex) {
    if (fieldIndex > fieldMappings.length) {      // should be >=
        throw new StarRocksPlannerException(...);
    }
    return fieldMappings[fieldIndex];             // AIOOBE when fieldIndex == length
}
```

Off by one: at exactly `length` the guard passes and the array access throws
`ArrayIndexOutOfBoundsException`, discarding the informative message the guard was written to give.
A negative index is not guarded at all. Worth fixing on its own -- it costs nothing and it is what
makes the next occurrence of this family diagnosable.

## Next steps

1. Fix the off-by-one (and the missing negative check). Independent, safe, improves diagnostics.
2. Minimise. The width of the outer relation looks required, so a synthetic wide table is probably
   the missing ingredient rather than a cleverer shape.
3. The real fix -- translating the index between scopes, or refusing to -- needs someone who knows
   the scope model. Until then note that the same path can return a wrong column silently.

## Recording gap this exposed

The soak's signature is `getStackTrace()[0]`, which is where the check FIRED. The frame that
matters, `visitSlot`, is one below and is not recorded anywhere; reports and logs are pruned after
about forty rounds, so a finding older than that has no stack left to read. `ExactMutantReplayProbe`
exists because of that: it replays a recorded statement against its corpus file and prints the first
twelve StarRocks frames.
