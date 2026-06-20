# Add Binance Exchange

Add Binance as a first-class exchange integration alongside Bybit and Deribit. Scope includes a new exchange module, host wiring, configuration, supported-exchange updates, and build verification.

## For Future Agents
As work proceeds: mark checkboxes `- [x]` as items complete; when a phase is done, set its status to `Complete` and write its **Phase Summary** (what was done, key decisions, anything needed to continue with zero context); run the phase's **Verification Plan** and record the result before moving on. When all phases are done, fill in **Final Recap** and **Deployment Plan**.

## Phase 1: Define Binance Integration Shape
Status: Complete

- [x] Inspect the shared exchange contract and existing Bybit and Deribit implementations.
- [x] Identify host-level registration, supported-exchange checks, and config sections that must be extended for Binance.
- [x] Confirm the Binance .NET client/API surface needed for tracked instruments, trade bootstrap, ticker snapshots, option snapshots, and live streams.

### Verification Plan
- Inspect `CryptoCollector.API.Exchange/Abstractions/IExchange.cs`, `CryptoCollector.Exchange.Bybit/`, `CryptoCollector.Exchange.Deribit/`, and `CryptoCollector.Api/Program.cs`; expected result: all Binance touchpoints are enumerated before edits begin.

### Phase Summary
Existing exchange touchpoints were mapped across the shared contract, exchange projects, and API host. The final shape uses `Binance.Net` for USD-M futures REST/websocket coverage and direct `eapi` REST/websocket handling for Binance options because the package does not expose a dedicated options client surface.

## Phase 2: Implement Binance Exchange Module
Status: Complete

- [x] Create `CryptoCollector.Exchange.Binance` project with options, service registration, and Binance client dependencies.
- [x] Implement Binance REST client logic for tracked instruments, recent trades, ticker snapshots, and option-chain snapshots.
- [x] Implement Binance exchange streaming logic that emits `ExchangeDataMessage` records for live trades and tickers.
- [x] Add the Binance project to the solution and API project references.

### Verification Plan
- Run `dotnet build CryptoCollector.slnx`; expected result: Binance project compiles and the solution builds cleanly.

### Phase Summary
Added a new `CryptoCollector.Exchange.Binance` project with DI registration, collector options, Binance options DTOs, a mixed REST client, and a concrete `BinanceExchange`. Futures instruments, bootstrap trades, snapshots, and live ticker/trade streams come from `Binance.Net`; option catalogs, option-chain snapshots, and live option trades come from Binance public `eapi`/websocket endpoints.

## Phase 3: Wire Binance Into The Host
Status: Complete

- [x] Register Binance services in `CryptoCollector.Api/Program.cs` and add a dedicated `ExchangeCollectorService` instance.
- [x] Extend supported-exchange checks and block-trade alert state initialization to include Binance.
- [x] Add Binance configuration defaults to `CryptoCollector.Api/appsettings*.json` as needed.

### Verification Plan
- Run `dotnet build CryptoCollector.slnx`; expected result: host wiring compiles with Binance enabled as a supported exchange.

### Phase Summary
The API host now references and registers Binance alongside Bybit and Deribit, spins a dedicated collector service for it, recognizes Binance in public history endpoints, and includes Binance in stateful trade dedupe / block-trade replay logic. Default Binance settings were added to the root appsettings and the Docker build inputs were extended to include the new project.

## Phase 4: Verify And Finalize
Status: Complete

- [x] Run targeted verification for the Binance integration and capture any required follow-up fixes.
- [x] Update this plan with completed phase summaries, final recap, and deployment notes.

### Verification Plan
- Run `dotnet build CryptoCollector.slnx`; expected result: final build succeeds after all edits.

### Phase Summary
Ran `dotnet build CryptoCollector.slnx` after integrating the new exchange and fixed the resulting compile-time issues in the Binance assembly. Final build completed successfully with zero warnings and zero errors.

## Final Recap
Binance is now a first-class exchange integration. The repository contains a new Binance exchange project, the API host registers and runs it, the solution and Docker build include it, and Binance is recognized everywhere the app tracks supported exchanges and replay state. Futures data uses `Binance.Net`; options data uses Binance public options REST/websocket endpoints.

## Deployment Plan
1. Build the solution with `dotnet build CryptoCollector.slnx`.
2. Start the API normally; the Binance collector will run automatically with the default `Binance` appsettings section.
3. Override the `Binance` configuration section in environment-specific settings if a different base asset, quote asset, or retry profile is needed.
4. Verify persisted output by querying the existing history endpoints with `exchange=binance` after the collector has been running long enough to flush data.
