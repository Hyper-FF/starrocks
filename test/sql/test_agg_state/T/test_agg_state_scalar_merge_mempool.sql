-- name: test_agg_state_scalar_merge_mempool
-- Regression for a BE data race in the scalar agg-state combinators. StateMergeFunction /
-- StateUnionFunction are a single object owned by the shared Expr and evaluated concurrently by
-- every pipeline driver, but they kept the nested FunctionContext (and its non-thread-safe MemPool)
-- used to drive the wrapped aggregate as a shared member. array_agg_distinct::merge allocates its
-- distinct-key storage from that MemPool, so concurrent execute() calls corrupted the shared pool.
-- The fix builds the nested context privately per execute() from the caller's per-driver context.
-- The aggregate form array_agg_distinct_merge (driven by the Aggregator with a per-driver pool) does
-- NOT exercise this; only the scalar _state_merge over a stored agg-state column does, which is why
-- the existing all-functions coverage did not catch it.
--
-- This case uses a large table and evaluates the scalar merge across many parallel pipeline drivers
-- (BUCKETS 8 + pipeline_dop 8), discarding the output through the blackhole sink so nothing
-- serializes back to the client and the merge runs at full concurrency. On the unfixed BE this
-- crashes; with the fix each worker gets its own nested context + private MemPool.
CREATE TABLE src (k INT, s VARCHAR(100))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 10000 groups x 40 distinct strings each = 400000 rows.
INSERT INTO src
SELECT g1.generate_series AS k,
       concat('str_', cast((g2.generate_series % 40) AS string)) AS s
FROM TABLE(generate_series(1, 10000)) g1, TABLE(generate_series(1, 40)) g2;

CREATE TABLE st (k INT, v array_agg_distinct(varchar(100)))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

INSERT INTO st SELECT k, array_agg_distinct_combine(s) FROM src GROUP BY k;

SET pipeline_dop = 8;

-- Concurrency stress: run the scalar state-merge across many parallel drivers, output discarded.
INSERT INTO blackhole() SELECT array_agg_distinct_state_merge(v) FROM st;

-- Deterministic, order-independent correctness check: every one of the 10000 groups merges back to
-- its 40 distinct strings (10000 * 40 = 400000).
SELECT count(*) AS n, sum(array_length(array_agg_distinct_state_merge(v))) AS total_elems
FROM st;
