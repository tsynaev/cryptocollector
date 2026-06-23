# Add Pre/Post Trade Open Interest To Trades

Add `preTradeOpenInterest` and `postTradeOpenInterest` to persisted trade records and downstream block-trade/history responses. Use high-frequency live option OI updates in memory as the enrichment source. Do not write those high-frequency OI updates to Parquet; Parquet remains on the existing coarse minute option-chain snapshot cadence.

## For Future Agents
As work proceeds: mark checkboxes `- [x]` as items complete; when a phase is done, set its status to `Complete` and write its **Phase Summary** (what was done, key decisions, anything needed to continue with zero context); run the phase's **Verification Plan** and record the result before moving on. When all phases are done, fill in **Final Recap** and **Deployment Plan**.

## Phase 1: Lock Semantics And Boundaries
Status: Not started

- [ ] Lock the field semantics:
  `preTradeOpenInterest` = the last in-memory OI event for the same `exchange + symbol` with `TimestampUtc <= trade.TradeTime`.
- [ ] Lock the field semantics:
  `postTradeOpenInterest` = the first in-memory OI event for the same `exchange + symbol` with `TimestampUtc > trade.TradeTime`.
- [ ] Lock the scope boundary:
  enrich only `InstrumentType.Option`; keep non-option trades as `null`.
- [ ] Lock the storage boundary:
  high-frequency OI history lives only in memory; only the enriched trade fields are persisted to Parquet.
- [ ] Lock the granularity caveat:
  this is event-based OI attribution, not an exchange-native `open/close` flag.
- [ ] Lock the runtime contract:
  `WaitForPostTradeTicker(tradeTime)` returns immediately if a later OI event already exists in memory, otherwise waits until `IExchange` pushes the next matching option OI event.
- [ ] Lock the ownership boundary:
  `MinuteAggregationService` owns OI enrichment and publishes a local `TradeExecuted` event only after the trade has been checked against the in-memory OI timeline, so downstream consumers do not implement their own waiter logic.

### Verification Plan
- Inspect current models and confirm option OI already exists in `ExchangeOptionTicker` and `OptionChainMinuteBar`.
- Confirm no existing trade model already stores pre/post OI fields.
- Record the chosen semantics in this file before editing code.

### Phase Summary
_(write when phase completes)_

## Phase 2: Extend Trade And API Models
Status: Not started

- [ ] Add nullable `PreTradeOpenInterest` and `PostTradeOpenInterest` to `ExchangeTrade`.
- [ ] Add nullable `PreTradeOpenInterest` and `PostTradeOpenInterest` to `TradeRecord`.
- [ ] Keep legacy trade record upgrades backward-compatible by leaving older schema readers untouched and letting missing columns deserialize as nulls.
- [ ] Add the same fields to outward-facing models that should expose them, including `BlockTradeHistoryLeg`.
- [ ] Verify Parquet schema migration still works with the new nullable trade columns.

### Verification Plan
- Build the solution successfully after model changes.
- Run a focused read path check against existing legacy trade parquet files and confirm deserialization still succeeds.

### Phase Summary
_(write when phase completes)_

## Phase 3: Add Reduced In-Memory OI Timeline
Status: Not started

- [ ] Add a reduced in-memory option ticker type with only:
  `TimestampUtc` and `OpenInterest`.
- [ ] Add a per-symbol concurrent timeline keyed by `exchange|symbol` that stores reduced OI events for only the trailing one minute.
- [ ] Add pruning so old OI events older than one minute are evicted on append.
- [ ] Add a per-symbol synchronization primitive so trade registration and OI event append are atomic for a given symbol and no post-OI event can be missed during waiter registration.
- [ ] Add a per-symbol waiter/pending-trade structure for trades whose `PostTradeOpenInterest` is still unresolved.
- [ ] Add a lightweight local message bus abstraction for in-process domain events, since the codebase does not already contain one.
- [ ] Keep the existing minute-bar aggregation unchanged; the reduced OI timeline is a separate memory-only path.

### Verification Plan
- Add unit tests for timeline append, one-minute pruning, atomic register-or-resolve behavior, and pending waiter bookkeeping.
- Build and inspect the new in-memory state flow.

### Phase Summary
_(write when phase completes)_

## Phase 4: Feed The Timeline From Live Option Updates
Status: Not started

- [ ] Ensure the exchange stream path emits `ExchangeOptionMessage` frequently enough to capture live OI changes, including websocket-driven option updates when available.
- [ ] In `IngestOption`, append a reduced OI event only when `OpenInterest` is non-null.
- [ ] Keep current option-chain minute aggregation for Parquet snapshots, but do not persist the high-frequency reduced events.
- [ ] Resolve any symbol normalization issues so trade symbols and option OI symbols match exactly for lookups.

### Verification Plan
- Add tests or a local harness proving `IngestOption` updates the in-memory OI timeline without changing Parquet write volume.
- Verify that trade symbol keys match option OI keys for Bybit option instruments.

### Phase Summary
_(write when phase completes)_

## Phase 5: Enrich Trades With Pre/Post OI
Status: Not started

- [ ] In `IngestTrade`, when `instrument.InstrumentType == Option`, set `PreTradeOpenInterest` from the last OI event at or before the trade time.
- [ ] In `IngestTrade`, try to resolve `PostTradeOpenInterest` immediately from the in-memory timeline if a later OI event already exists.
- [ ] If no later OI event exists yet, register the trade with `WaitForPostTradeTicker(tradeTime)` semantics under the same per-symbol lock that guards OI event append, so an OI event arriving during registration cannot be lost.
- [ ] On each new OI event, resolve all pending trades for that symbol with `tradeTime < oiEvent.TimestampUtc` and fill `PostTradeOpenInterest`.
- [ ] Document and test that multiple trades before the same next OI event can legitimately receive the same `PostTradeOpenInterest`.
- [ ] Publish a local `TradeExecuted` event from `MinuteAggregationService` after the trade has final `PreTradeOpenInterest` and either resolved `PostTradeOpenInterest` or an explicitly pending/null state that downstream services accept by contract.

### Verification Plan
- Add unit tests covering:
  pre-OI lookup,
  immediate post-OI resolution,
  delayed waiter resolution,
  no lost resolution when OI append races with waiter registration,
  non-option trades remaining null,
  multiple trades resolving against one later OI event.
- Run the full test project containing `MinuteAggregationService` tests.

### Phase Summary
_(write when phase completes)_

## Phase 6: Persist Backfilled Post OI Reliably
Status: Not started

- [ ] Ensure unresolved option trades remain mutable in memory until either `PostTradeOpenInterest` is filled or the trade row is flushed.
- [ ] Audit the current flush cadence against 100ms OI events and determine whether most trades will receive `PostTradeOpenInterest` before flush.
- [ ] If a trade can flush before post-OI arrives, add an explicit trade-row update/rewrite path in `DailyParquetStore` keyed by `exchange + symbol + tradeId + timestamp`.
- [ ] Ensure the persistence path works for both live trades and recovered trades ingested during bootstrap catch-up.
- [ ] Add cleanup for trades that never receive a later OI event within the retained memory window so persistence does not stall indefinitely.

### Verification Plan
- Run an integration-style test where a trade ingests first, a later OI event arrives, and the stored `TradeRecord` contains both pre and post OI.
- Test the edge case where the trade flushes before post-OI arrives and verify the rewrite/update path persists the backfill.

### Phase Summary
_(write when phase completes)_

## Phase 7: Surface The Fields Through History And Alerts
Status: Not started

- [ ] Add `PreTradeOpenInterest` and `PostTradeOpenInterest` to `BlockTradeHistoryLeg`.
- [ ] Update `BlockTradeHistoryBuilder.MapLeg` to copy the new fields from `TradeRecord`.
- [ ] Expose the new fields through any endpoints that serialize `TradeRecord` or grouped block-trade legs.
- [ ] Change `BlockTradeAlertService` to consume the local `TradeExecuted` event instead of raw exchange trades, so it always sees the enriched trade shape.
- [ ] Decide whether alert text remains unchanged or begins printing OI diagnostics; keep API fields even if alert text stays compact.
- [ ] Document that `postTradeOpenInterest` can remain null for very recent trades or when no later OI event was observed.

### Verification Plan
- Query the local history endpoints and confirm the new fields serialize for option trades.
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
