# Elle Write Skew Test Results — March 25, 2026

4 threads, 500 rounds each, 5 accounts x 200 initial = 1000 total.
Withdraw 100 if total >= 100. Under serializable isolation, concurrent
withdrawals that would violate the invariant must be aborted.

| Engine | Mode | Commits | Aborts | Min Balance | Write Skew? |
|--------|------|---------|--------|-------------|-------------|
| SQLite | BEGIN (deferred) | 1,997 | 0 | -200 | Yes (expected — not serializable) |
| SQLite | BEGIN IMMEDIATE | 1,995 | 0 | -400 | Yes (expected — not serializable) |
| Limbo | BEGIN (deferred) | 2,000 | 0 | -100 | Yes (expected — not serializable) |
| **FrankenSQLite** | **BEGIN (autocommit)** | **2,000** | **0** | **-200** | **Yes** |
| **FrankenSQLite** | **BEGIN CONCURRENT** | **2,000** | **0** | **-300** | **Yes — claims SSI, doesn't enforce it** |

## Key finding

FrankenSQLite's BEGIN CONCURRENT claims serializable snapshot isolation.
A real SSI implementation (PostgreSQL, CockroachDB) would detect the
read-write conflicts and abort some transactions, preventing the total
from going negative.

FrankenSQLite with BEGIN CONCURRENT:
- Zero aborts (conflict detection not firing)
- Minimum balance -300 (invariant violated)
- The SSI machinery is ON (concurrent_mode_default = true)
- The global mutex serializes commit timing but does NOT prevent the
  write skew because each transaction reads a snapshot, decides to
  withdraw, and commits independently

This is not just a performance issue. It is a data integrity issue.
The database claims serializable isolation and allows write skew.

## Caveat

SQLite and Limbo also allow write skew because they do not claim
serializable isolation at these transaction levels. Their behavior
is correct for their claimed isolation level (read-committed /
snapshot isolation). FrankenSQLite's behavior is incorrect because
it claims SSI via BEGIN CONCURRENT.

## Note on test design

This test uses `BEGIN` (deferred) for SQLite/Limbo, which is
read-committed level. Neither claims serializable at this level.
The write skew is expected and correct behavior for them.

FrankenSQLite's `BEGIN CONCURRENT` is documented as providing
serializable snapshot isolation with conflict detection. The write
skew under BEGIN CONCURRENT is the anomaly.
