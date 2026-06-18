using Bybit.Net.Clients;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;
using CryptoCollector.Exchange.Bybit.Options;
using CryptoExchange.Net.Objects.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Exchange.Bybit.Services;

public sealed class BybitExchange(
    BybitApiClient apiClient,
    BybitSocketClient socketClient,
    IOptions<BybitCollectorOptions> options,
    ILogger<BybitExchange> logger) : IExchange
{
    private readonly BybitCollectorOptions _options = options.Value;

    public string Name => "bybit";
    public TimeSpan ReconnectDelay => _options.ReconnectDelay;
    public TimeSpan OptionChainSnapshotInterval => TimeSpan.FromSeconds(60);

    public Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(CancellationToken cancellationToken) =>
        apiClient.GetTrackedInstrumentsAsync(_options.BaseAsset, _options.QuoteAsset, cancellationToken);

    public async Task BootstrapAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken)
    {
        await BootstrapLinearTickersAsync(instruments, sink, cancellationToken);
        await BootstrapLinearTradesAsync(instruments, sink, cancellationToken);
        await BootstrapOptionTradesAsync(instruments, sink, cancellationToken);
    }

    public async Task PollOptionChainSnapshotsAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.Category.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        var snapshots = await apiClient.GetOptionChainSnapshotsAsync(cancellationToken);
        foreach (var snapshot in snapshots)
        {
            if (trackedSymbols.TryGetValue(snapshot.Symbol, out var instrument))
            {
                sink.IngestTicker(instrument, snapshot.Payload, snapshot.TimestampUtc);
            }
        }
    }

    public async Task StreamAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken)
    {
        var subscriptions = new List<UpdateSubscription>();
        var linearInstruments = instruments.Where(static x => x.Category.Equals("linear", StringComparison.OrdinalIgnoreCase));
        var optionSymbols = instruments
            .Where(static x => x.Category.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var linearSymbols = linearInstruments.ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var connectionClosedSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            foreach (var chunk in linearInstruments.Select(static x => x.Symbol).Chunk(_options.LinearChunkSize))
            {
                subscriptions.Add(await SubscribeAsync(
                    socketClient.V5LinearApi.SubscribeToTickerUpdatesAsync(
                        chunk,
                        update =>
                        {
                            if (linearSymbols.TryGetValue(update.Data.Symbol, out var instrument))
                            {
                                sink.IngestTicker(instrument, update.Data, DateTimeOffset.UtcNow);
                            }
                        },
                        cancellationToken)));

                subscriptions.Add(await SubscribeAsync(
                    socketClient.V5LinearApi.SubscribeToTradeUpdatesAsync(
                        chunk,
                        update =>
                        {
                            foreach (var trade in update.Data)
                            {
                                if (linearSymbols.TryGetValue(trade.Symbol, out var instrument))
                                {
                                    sink.IngestTrade(instrument, trade);
                                }
                            }
                        },
                        cancellationToken)));
            }

            subscriptions.Add(await SubscribeAsync(
                socketClient.V5OptionsApi.SubscribeToTradeUpdatesAsync(
                    _options.BaseAsset,
                    update =>
                    {
                        foreach (var trade in update.Data)
                        {
                            if (optionSymbols.TryGetValue(trade.Symbol, out var instrument))
                            {
                                sink.IngestTrade(instrument, trade);
                            }
                        }
                    },
                    cancellationToken)));

            foreach (var subscription in subscriptions)
            {
                subscription.ConnectionLost += () => logger.LogWarning("Bybit websocket connection lost. Waiting for automatic recovery.");
                subscription.ResubscribingFailed += error =>
                {
                    logger.LogWarning("Bybit websocket automatic resubscribe failed: {Error}.", error);
                    connectionClosedSignal.TrySetResult();
                };
                subscription.ConnectionClosed += () => connectionClosedSignal.TrySetResult();
            }

            await connectionClosedSignal.Task.WaitAsync(cancellationToken);
        }
        finally
        {
            foreach (var subscription in subscriptions)
            {
                try
                {
                    await socketClient.UnsubscribeAsync(subscription);
                }
                catch (Exception exception)
                {
                    logger.LogWarning(exception, "Bybit websocket unsubscribe failed.");
                }
            }
        }
    }

    private async Task BootstrapLinearTickersAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.Category.Equals("linear", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        var tickers = await apiClient.GetLinearTickersAsync(_options.BaseAsset, cancellationToken);
        var timestamp = DateTimeOffset.UtcNow;

        foreach (var ticker in tickers)
        {
            if (trackedSymbols.TryGetValue(ticker.Symbol, out var instrument))
            {
                sink.IngestTicker(instrument, ticker, timestamp);
            }
        }
    }

    private async Task BootstrapLinearTradesAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken)
    {
        foreach (var instrument in instruments.Where(static x => x.Category.Equals("linear", StringComparison.OrdinalIgnoreCase)))
        {
            var trades = await apiClient.GetRecentLinearTradesAsync(instrument.Symbol, cancellationToken);
            foreach (var trade in trades)
            {
                sink.IngestTrade(instrument, trade);
            }
        }
    }

    private async Task BootstrapOptionTradesAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.Category.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        var trades = await apiClient.GetRecentOptionTradesAsync(_options.BaseAsset, cancellationToken);
        foreach (var trade in trades)
        {
            if (trackedSymbols.TryGetValue(trade.Symbol, out var instrument))
            {
                sink.IngestTrade(instrument, trade);
            }
        }
    }

    private async Task<UpdateSubscription> SubscribeAsync(Task<CryptoExchange.Net.Objects.CallResult<UpdateSubscription>> task)
    {
        var result = await task;
        if (!result.Success || result.Data is null)
        {
            logger.LogError("Bybit websocket subscribe failed. Error={Error}.", result.Error);
            throw new InvalidOperationException($"Subscription failed: {result.Error}");
        }

        return result.Data;
    }
}
