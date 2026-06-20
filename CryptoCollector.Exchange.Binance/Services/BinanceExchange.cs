using Binance.Net.Clients;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Binance.Models;
using CryptoCollector.Exchange.Binance.Options;
using CryptoExchange.Net.Objects.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace CryptoCollector.Exchange.Binance.Services;

public sealed class BinanceExchange(
    BinanceApiClient apiClient,
    BinanceSocketClient socketClient,
    IOptions<BinanceCollectorOptions> options,
    ILogger<BinanceExchange> logger) : IExchange
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = false
    };
    private readonly BinanceCollectorOptions _options = options.Value;

    public string Name => "binance";
    public TimeSpan ReconnectDelay => _options.ReconnectDelay;
    public TimeSpan OptionChainSnapshotInterval => TimeSpan.FromSeconds(60);

    public Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(CancellationToken cancellationToken) =>
        apiClient.GetTrackedInstrumentsAsync(_options.BaseAsset, _options.QuoteAsset, cancellationToken);

    public async IAsyncEnumerable<ExchangeTradeMessage> StreamTradesSinceAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        DateTime? catchUpFromUtc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var instrument in instruments.Where(static x => x.InstrumentType != InstrumentType.Option))
        {
            var trades = await apiClient.GetRecentFutureTradesAsync(instrument.Symbol, cancellationToken);
            foreach (var trade in trades)
            {
                if (catchUpFromUtc is not null && trade.TradeTime <= catchUpFromUtc.Value)
                {
                    continue;
                }

                yield return new ExchangeTradeMessage(instrument, BinanceApiClient.MapFutureTrade(instrument.Symbol, trade));
            }
        }

        var optionBootstrapTrades = await apiClient.GetRecentActiveOptionTradesAsync(instruments, catchUpFromUtc, cancellationToken);
        foreach (var entry in optionBootstrapTrades
                     .OrderBy(static x => x.Trade.TradeTime))
        {
            yield return new ExchangeTradeMessage(entry.Instrument, entry.Trade);
        }
    }

    public async IAsyncEnumerable<ExchangeTickerMessage> StreamTickersSnapshotAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.InstrumentType != InstrumentType.Option)
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var tickers = await apiClient.GetFuturesTickerSnapshotsAsync(cancellationToken);

        foreach (var ticker in tickers)
        {
            if (trackedSymbols.TryGetValue(ticker.Symbol, out var instrument))
            {
                yield return new ExchangeTickerMessage(instrument, ticker.Ticker);
            }
        }
    }

    public async IAsyncEnumerable<ExchangeOptionMessage> StreamOptionChainSnapshotAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var trackedSymbols = instruments
            .Where(static x => x.InstrumentType == InstrumentType.Option)
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var snapshots = await apiClient.GetOptionChainSnapshotsAsync(cancellationToken);
        foreach (var snapshot in snapshots)
        {
            if (trackedSymbols.TryGetValue(snapshot.Symbol, out var instrument))
            {
                yield return new ExchangeOptionMessage(instrument, snapshot.Ticker);
            }
        }
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
        var futures = instruments.Where(static x => x.InstrumentType != InstrumentType.Option).ToArray();
        var optionSymbols = instruments
            .Where(static x => x.InstrumentType == InstrumentType.Option)
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var futureSymbols = futures.ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var subscriptions = new List<UpdateSubscription>();
        var connectionClosedSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? optionsTradeStreamTask = null;

        try
        {
            foreach (var chunk in futures.Select(static x => x.Symbol).Chunk(_options.FuturesSubscriptionChunkSize))
            {
                subscriptions.Add(await SubscribeAsync(
                    socketClient.UsdFuturesApi.ExchangeData.SubscribeToTickerUpdatesAsync(
                        chunk,
                        update =>
                        {
                            if (futureSymbols.TryGetValue(update.Data.Symbol, out var instrument))
                            {
                                writer.TryWrite(new ExchangeTickerMessage(instrument, BinanceApiClient.MapFuturesTicker(update.Data, DateTimeOffset.UtcNow)));
                            }
                        },
                        cancellationToken)));

                subscriptions.Add(await SubscribeAsync(
                    socketClient.UsdFuturesApi.ExchangeData.SubscribeToMarkPriceUpdatesAsync(
                        chunk,
                        1000,
                        update =>
                        {
                            if (futureSymbols.TryGetValue(update.Data.Symbol, out var instrument))
                            {
                                writer.TryWrite(new ExchangeTickerMessage(instrument, BinanceApiClient.MapFuturesTicker(update.Data, DateTimeOffset.UtcNow)));
                            }
                        },
                        cancellationToken)));

                subscriptions.Add(await SubscribeAsync(
                    socketClient.UsdFuturesApi.ExchangeData.SubscribeToBookTickerUpdatesAsync(
                        chunk,
                        update =>
                        {
                            if (futureSymbols.TryGetValue(update.Data.Symbol, out var instrument))
                            {
                                writer.TryWrite(new ExchangeTickerMessage(instrument, BinanceApiClient.MapFuturesTicker(update.Data, DateTimeOffset.UtcNow)));
                            }
                        },
                        cancellationToken)));

                subscriptions.Add(await SubscribeAsync(
                    socketClient.UsdFuturesApi.ExchangeData.SubscribeToTradeUpdatesAsync(
                        chunk,
                        update =>
                        {
                            if (futureSymbols.TryGetValue(update.Data.Symbol, out var instrument))
                            {
                                writer.TryWrite(new ExchangeTradeMessage(instrument, BinanceApiClient.MapFutureTrade(update.Data)));
                            }
                        },
                        true,
                        cancellationToken)));
            }

            if (optionSymbols.Count > 0)
            {
                optionsTradeStreamTask = RunOptionsTradeStreamAsync(optionSymbols, writer, connectionClosedSignal, cancellationToken);
            }

            foreach (var subscription in subscriptions)
            {
                subscription.ConnectionLost += () => logger.LogWarning("Binance websocket connection lost. Waiting for automatic recovery.");
                subscription.ResubscribingFailed += error =>
                {
                    logger.LogWarning("Binance websocket automatic resubscribe failed: {Error}.", error);
                    connectionClosedSignal.TrySetResult();
                };
                subscription.ConnectionClosed += () => connectionClosedSignal.TrySetResult();
            }

            await connectionClosedSignal.Task.WaitAsync(cancellationToken);
            if (optionsTradeStreamTask is not null)
            {
                await optionsTradeStreamTask;
            }
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
                    logger.LogWarning(exception, "Binance websocket unsubscribe failed.");
                }
            }

            writer.TryComplete();
        }
    }

    private async Task RunOptionsTradeStreamAsync(
        IReadOnlyDictionary<string, InstrumentDefinition> optionSymbols,
        ChannelWriter<ExchangeDataMessage> writer,
        TaskCompletionSource connectionClosedSignal,
        CancellationToken cancellationToken)
    {
        using var socket = new ClientWebSocket();
        var uri = new Uri($"{_options.OptionsWebSocketBaseUrl.TrimEnd('/')}/public/ws/{_options.UnderlyingSymbol.ToLowerInvariant()}@optionTrade");
        await socket.ConnectAsync(uri, cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            string? message;
            try
            {
                message = await ReceiveMessageAsync(socket, cancellationToken);
            }
            catch (WebSocketException exception)
            {
                logger.LogWarning(exception, "Binance options websocket receive failed. State={State}, CloseStatus={CloseStatus}, Description={Description}.",
                    socket.State,
                    socket.CloseStatus,
                    socket.CloseStatusDescription);
                break;
            }

            if (message is null)
            {
                logger.LogWarning("Binance options websocket closed. State={State}, CloseStatus={CloseStatus}, Description={Description}.",
                    socket.State,
                    socket.CloseStatus,
                    socket.CloseStatusDescription);
                break;
            }

            var trade = JsonSerializer.Deserialize<BinanceOptionTradeStreamMessage>(message, JsonOptions);
            if (trade is null || !optionSymbols.TryGetValue(trade.Symbol, out var instrument))
            {
                continue;
            }

            writer.TryWrite(new ExchangeTradeMessage(instrument, BinanceApiClient.MapOptionTrade(trade)));
        }

        if (socket.State == WebSocketState.Open)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "shutdown", CancellationToken.None);
        }

        connectionClosedSignal.TrySetResult();
    }

    private async Task<UpdateSubscription> SubscribeAsync(Task<CryptoExchange.Net.Objects.CallResult<UpdateSubscription>> task)
    {
        var result = await task;
        if (!result.Success || result.Data is null)
        {
            logger.LogError("Binance websocket subscribe failed. Error={Error}.", result.Error);
            throw new InvalidOperationException($"Subscription failed: {result.Error}");
        }

        return result.Data;
    }

    private async Task<string?> ReceiveMessageAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[_options.WebSocketReceiveBufferSize];
        using var stream = new MemoryStream();

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return null;
            }

            stream.Write(buffer, 0, result.Count);
            if (result.EndOfMessage)
            {
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }
    }
}
