using Bybit.Net.Clients;
using Bybit.Net.Objects.Models.V5;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Bybit.Options;
using CryptoExchange.Net.Objects.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

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

    public async Task<ExchangeBootstrapBatch> BootstrapAsync(IReadOnlyList<InstrumentDefinition> instruments, DateTime? catchUpFromUtc, CancellationToken cancellationToken) =>
        new()
        {
            Tickers = await BootstrapLinearTickersAsync(instruments, cancellationToken),
            Trades = (await BootstrapLinearTradesAsync(instruments, catchUpFromUtc, cancellationToken))
                .Concat(await BootstrapOptionTradesAsync(instruments, catchUpFromUtc, cancellationToken))
                .ToArray()
        };

    public async Task<IReadOnlyList<ExchangeOptionMessage>> PollOptionChainSnapshotsAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.Category.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var result = new List<ExchangeOptionMessage>();

        var snapshots = await apiClient.GetOptionChainSnapshotsAsync(cancellationToken);
        foreach (var snapshot in snapshots)
        {
            if (trackedSymbols.TryGetValue(snapshot.Symbol, out var instrument))
            {
                result.Add(new ExchangeOptionMessage(instrument, snapshot.Ticker));
            }
        }

        return result;
    }

    public async IAsyncEnumerable<ExchangeDataMessage> StreamAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateUnbounded<ExchangeDataMessage>();
        var producer = RunStreamAsync(instruments, channel.Writer, cancellationToken);

        await foreach (var message in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return message;
        }

        await producer;
    }

    private async Task RunStreamAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        ChannelWriter<ExchangeDataMessage> writer,
        CancellationToken cancellationToken)
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
                                    writer.TryWrite(new ExchangeTickerMessage(instrument, BybitApiClient.MapLinearTicker(update.Data, DateTimeOffset.UtcNow)));
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
                                    writer.TryWrite(new ExchangeTradeMessage(instrument, MapTrade(trade)));
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
                                writer.TryWrite(new ExchangeTradeMessage(instrument, MapTrade(trade)));
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
        catch (Exception exception)
        {
            writer.TryComplete(exception);
            throw;
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

            writer.TryComplete();
        }
    }

    private async Task<IReadOnlyList<ExchangeTickerMessage>> BootstrapLinearTickersAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.Category.Equals("linear", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var result = new List<ExchangeTickerMessage>();

        var tickers = await apiClient.GetLinearTickersAsync(_options.BaseAsset, cancellationToken);
        var timestamp = DateTimeOffset.UtcNow;

        foreach (var ticker in tickers)
        {
            if (trackedSymbols.TryGetValue(ticker.Symbol, out var instrument))
            {
                result.Add(new ExchangeTickerMessage(instrument, BybitApiClient.MapLinearTicker(ticker, timestamp)));
            }
        }

        return result;
    }

    private async Task<IReadOnlyList<ExchangeTradeMessage>> BootstrapLinearTradesAsync(IReadOnlyList<InstrumentDefinition> instruments, DateTime? catchUpFromUtc, CancellationToken cancellationToken)
    {
        var result = new List<ExchangeTradeMessage>();

        foreach (var instrument in instruments.Where(static x => x.Category.Equals("linear", StringComparison.OrdinalIgnoreCase)))
        {
            var trades = await apiClient.GetRecentLinearTradesAsync(instrument.Symbol, cancellationToken);
            foreach (var trade in trades)
            {
                if (catchUpFromUtc is not null && trade.Timestamp <= catchUpFromUtc.Value)
                {
                    continue;
                }

                result.Add(new ExchangeTradeMessage(instrument, MapTrade(trade)));
            }
        }

        return result;
    }

    private async Task<IReadOnlyList<ExchangeTradeMessage>> BootstrapOptionTradesAsync(IReadOnlyList<InstrumentDefinition> instruments, DateTime? catchUpFromUtc, CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.Category.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var result = new List<ExchangeTradeMessage>();

        var trades = await apiClient.GetRecentOptionTradesAsync(_options.BaseAsset, cancellationToken);
        foreach (var trade in trades)
        {
            if (catchUpFromUtc is not null && trade.Timestamp <= catchUpFromUtc.Value)
            {
                continue;
            }

            if (trackedSymbols.TryGetValue(trade.Symbol, out var instrument))
            {
                result.Add(new ExchangeTradeMessage(instrument, MapTrade(trade)));
            }
        }

        return result;
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

    private static ExchangeTrade MapTrade(BybitTrade trade) =>
        new()
        {
            TradeTime = new DateTimeOffset(trade.Timestamp),
            Symbol = trade.Symbol,
            Side = trade.Side.ToString(),
            Quantity = trade.Quantity,
            Price = trade.Price,
            TradeId = trade.TradeId,
            IsBlockTrade = trade.IsBlockTrade ?? false,
            BlockTradeId = null,
            IsRpiTrade = trade.IsRpiTrade ?? false,
            Sequence = (trade.Sequence ?? 0).ToString(System.Globalization.CultureInfo.InvariantCulture)
        };

    private static ExchangeTrade MapTrade(BybitTradeHistory trade) =>
        new()
        {
            TradeTime = new DateTimeOffset(trade.Timestamp),
            Symbol = trade.Symbol,
            Side = trade.Side.ToString(),
            Quantity = trade.Quantity,
            Price = trade.Price,
            TradeId = trade.TradeId,
            IsBlockTrade = trade.IsBlockTrade,
            BlockTradeId = null,
            IsRpiTrade = trade.IsRpiTrade ?? false,
            Sequence = (trade.Sequence ?? 0).ToString(System.Globalization.CultureInfo.InvariantCulture)
        };

}
