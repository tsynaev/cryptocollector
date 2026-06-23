# Add Pre/Post Trade Open Interest To Trades

Add OI-based enrichment only for block/large derivative trade candidates, not for every raw trade. Use high-frequency live OI updates in memory as the enrichment source, with the exact OI feed depending on instrument type. Do not write those high-frequency OI updates to Parquet; Parquet remains on the existing coarse minute snapshot cadence.

## For Future Agents
As work proceeds: mark checkboxes `- [x]` as items complete; when a phase is done, set its status to `Complete` and write its **Phase Summary** (what was done, key decisions, anything needed to continue with zero context); run the phase's **Verification Plan** and record the result before moving on. When all phases are done, fill in **Final Recap** and **Deployment Plan**.

## Phase 1: Lock Semantics And Boundaries
Status: Not started

- [ ] Lock the field semantics:
  `preTradeOpenInterest` = the last in-memory OI event for the same `exchange + symbol` with `TimestampUtc <= trade.TradeTime`.
- [ ] Lock the field semantics:
  `postTradeOpenInterest` = the first in-memory OI event for the same `exchange + symbol` with `TimestampUtc > trade.TradeTime`.
- [ ] Lock the scope boundary:
  enrich only alert-worthy derivative trades; ordinary raw trades remain unchanged.
- [ ] Lock the storage boundary:
  high-frequency OI history lives only in memory; ordinary trade parquet rows do not gain `pre/post OI`.
- [ ] Lock the instrument boundary:
  candidate selection is instrument-agnostic across large derivative trades, but OI enrichment uses an instrument-specific feed:
  option OI from option updates, futures/perps OI from derivative ticker updates.
- [ ] Lock the granularity caveat:
  this is event-based OI attribution, not an exchange-native `open/close` flag.
- [ ] Lock the runtime contract:
  `WaitForPostTradeTicker(tradeTime)` returns immediately if a later OI event already exists in memory, otherwise waits until `IExchange` pushes the next matching option OI event.
- [ ] Lock the ownership boundary:
  the enrichment owner publishes a local enriched candidate event only after the trade has been checked against the in-memory OI timeline, so downstream consumers do not implement their own waiter logic.
- [ ] Lock the candidate filter boundary:
  only trades that already qualify as block/large candidates enter OI enrichment and pending resolution.

### Verification Plan
- Inspect current models and confirm option OI already exists in `ExchangeOptionTicker` and `OptionChainMinuteBar`.
- Confirm no existing trade model already stores pre/post OI fields.
- Record the chosen semantics in this file before editing code.

### Phase Summary
_(write when phase completes)_

## Phase 2: Add Candidate Enrichment Models
Status: Not started

- [ ] Keep `ExchangeTrade` and `TradeRecord` unchanged for ordinary trade ingestion.
- [ ] Add a dedicated enriched candidate model carrying:
  raw trade identity, `PreTradeOpenInterest`, `PostTradeOpenInterest`, and derived OI delta/status.
- [ ] Add any outward-facing models that should expose candidate OI enrichment, without bloating the generic trade schema.
- [ ] Add a dedicated persisted model for enriched block-trade candidates so `pre/post OI` lives in a separate Parquet dataset from generic trades.

### Verification Plan
- Build the solution successfully after model changes.
- Confirm no Parquet trade schema migration is needed for ordinary trade rows.

### Phase Summary
_(write when phase completes)_

## Phase 3: Add Reduced In-Memory OI Timeline
Status: Not started

- [ ] Add a reduced in-memory OI event type with only:
  `TimestampUtc` and `OpenInterest`.
- [ ] Add a per-symbol concurrent timeline keyed by `exchange|symbol` that stores reduced OI events for only the trailing retention window.
- [ ] Add pruning so old OI events older than the retention window are evicted on append.
- [ ] Add a per-symbol synchronization primitive so trade registration and OI event append are atomic for a given symbol and no post-OI event can be missed during waiter registration.
- [ ] Add a per-symbol waiter/pending-trade structure for trades whose `PostTradeOpenInterest` is still unresolved.
- [ ] Add a lightweight local message bus abstraction for in-process domain events, since the codebase does not already contain one.
- [ ] Keep the existing minute-bar aggregation unchanged; the reduced OI timeline is a separate memory-only path.

### Verification Plan
- Add unit tests for timeline append, one-minute pruning, atomic register-or-resolve behavior, and pending waiter bookkeeping.
- Build and inspect the new in-memory state flow.

### Phase Summary
_(write when phase completes)_

## Phase 4: Feed The Timeline From Live OI Updates
Status: Not started

- [ ] Ensure the exchange stream path emits OI-bearing messages frequently enough to capture live OI changes for each derivative instrument type.
- [ ] In `IngestOption`, append a reduced OI event only when `OpenInterest` is non-null.
- [ ] In `IngestTicker`, append a reduced OI event for futures/perps only when `OpenInterest` is non-null.
- [ ] Keep current minute aggregation for Parquet snapshots, but do not persist the high-frequency reduced events.
- [ ] Resolve any symbol normalization issues so trade symbols and OI symbols match exactly for lookups across options, futures, and perps.

### Verification Plan
- Add tests or a local harness proving `IngestOption` updates the in-memory OI timeline without changing Parquet write volume.
- Add tests or a local harness proving `IngestTicker` updates the in-memory OI timeline for futures/perps without changing Parquet write volume.
- Verify that trade symbol keys match OI keys for Bybit option and derivative instruments.

### Phase Summary
_(write when phase completes)_

## Phase 5: Enrich Only Candidate Trades With Pre/Post OI
Status: Not started

- [ ] Add a candidate filter in the ingestion path so only block/large derivative trades enter OI enrichment.
- [ ] For candidate trades, set `PreTradeOpenInterest` from the last OI event at or before the trade time.
- [ ] For candidate trades, try to resolve `PostTradeOpenInterest` immediately from the in-memory timeline if a later OI event already exists.
- [ ] If no later OI event exists yet, register only the candidate trade with `WaitForPostTradeTicker(tradeTime)` semantics under the same per-symbol lock that guards OI event append, so an OI event arriving during registration cannot be lost.
- [ ] On each new OI event, resolve all pending candidate trades for that symbol with `tradeTime < oiEvent.TimestampUtc` and fill `PostTradeOpenInterest`.
- [ ] Document and test that multiple candidate trades before the same next OI event can legitimately receive the same `PostTradeOpenInterest`.
- [ ] Publish a local enriched candidate event after the trade has been evaluated against OI history and either resolved or intentionally marked pending.

### Verification Plan
- Add unit tests covering:
  candidate filtering,
  pre-OI lookup,
  immediate post-OI resolution,
  delayed waiter resolution,
  no lost resolution when OI append races with waiter registration,
  ordinary/small trades bypassing enrichment,
  futures/perps candidate enrichment through ticker-based OI,
  multiple candidate trades resolving against one later OI event.
- Run the full test project containing `MinuteAggregationService` tests.

### Phase Summary
_(write when phase completes)_

## Phase 6: Persist Enriched Block Trades Reliably
Status: Not started

- [ ] Add a dedicated Parquet dataset for enriched block/large trade candidates, separate from generic trade parquet rows.
- [ ] Ensure unresolved candidate trades remain mutable in memory until either `PostTradeOpenInterest` is filled or the candidate expires.
- [ ] Define the persisted schema to include trade identity, grouping fields, `PreTradeOpenInterest`, `PostTradeOpenInterest`, and derived OI delta/status.
- [ ] Audit the current flush cadence against 100ms OI events and determine the dedicated write path for the enriched dataset.
- [ ] Ensure the chosen routing/persistence path works for both live trades and recovered trades ingested during bootstrap catch-up.
- [ ] Add cleanup for candidate trades that never receive a later OI event within the retained memory window so pending state does not grow unbounded.

### Verification Plan
- Run an integration-style test where a candidate trade ingests first, a later OI event arrives, and the enriched block-trade Parquet row contains both pre and post OI.
- Test the edge case where candidate emission/persistence spans multiple flush cycles and verify no enriched block trade is lost.

### Phase Summary
_(write when phase completes)_

## Phase 7: Surface The Fields Through History And Alerts
Status: Not started

- [ ] Add `PreTradeOpenInterest` and `PostTradeOpenInterest` to `BlockTradeHistoryLeg`.
- [ ] Update `BlockTradeHistoryBuilder.MapLeg` from the enriched block-trade dataset instead of generic `TradeRecord` when OI fields are needed.
- [ ] Expose the new fields only through alert/block-trade-specific models or endpoints, not generic trade history.
- [ ] Change `BlockTradeAlertService` to consume the local enriched candidate event instead of raw exchange trades, so it always sees the enriched trade shape.
- [ ] Decide whether alert text remains unchanged or begins printing OI diagnostics; keep API fields even if alert text stays compact.
- [ ] Document that `postTradeOpenInterest` can remain null for very recent trades or when no later OI event was observed.

### Verification Plan
- Query the local history endpoints and confirm the new fields serialize for any supported enriched derivative candidate type.
- Add or update endpoint tests if present.

### Phase Summary
_(write when phase completes)_

## Phase 8: Historical Backfill Policy
Status: Not started

- [ ] Decide whether historical parquet trade files need backfill or whether the feature applies only to newly ingested trades.
- [ ] If historical backfill is required, implement an offline job that reconstructs pre/post OI from stored minute option-chain snapshots with explicitly lower fidelity than live event-based enrichment.
- [ ] Store backfill scripts and scratch artifacts under `_temp/` only.
- [ ] Document the fidelity gap between live in-memory enrichment and historical snapshot-based backfill.

### Verification Plan
- Run the backfill job on a narrow date range and inspect sampled records before and after.
- Confirm no temporary artifacts are written outside `_temp/`.

### Phase Summary
_(write when phase completes)_

## Final Recap
_(write when all phases complete: summary of the entire piece of work)_

## Deployment Plan
_(write when all phases complete: step-by-step deployment instructions)_
