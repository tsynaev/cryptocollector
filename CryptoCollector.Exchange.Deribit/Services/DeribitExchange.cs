using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Deribit.Models;
using CryptoCollector.Exchange.Deribit.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Exchange.Deribit.Services;

public sealed class DeribitExchange(
    DeribitApiClient apiClient,
    IOptions<DeribitCollectorOptions> options,
    ILogger<DeribitExchange> logger) : IExchange
{
    private readonly DeribitCollectorOptions _options = options.Value;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public string Name => "deribit";
    public TimeSpan ReconnectDelay => _options.ReconnectDelay;
    public TimeSpan OptionChainSnapshotInterval => TimeSpan.FromSeconds(60);

    public Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(CancellationToken cancellationToken) =>
        apiClient.GetTrackedInstrumentsAsync(_options.BaseAsset, _options.QuoteAsset, cancellationToken);

    public async IAsyncEnumerable<ExchangeTradeMessage> StreamTradesSinceAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        DateTime? catchUpFromUtc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var bootstrapFromUtc = catchUpFromUtc ?? DateTime.UtcNow.AddHours(-24);
        var bootstrapToUtc = DateTime.UtcNow;
        var optionSymbols = instruments
            .Where(static x => x.Category.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var futureSymbols = instruments
            .Where(static x => x.Category.Equals("future", StringComparison.OrdinalIgnoreCase) || x.Category.Equals("perpetual", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        await foreach (var trade in apiClient.StreamOptionTradesAsync(_options.BaseAsset, bootstrapFromUtc, bootstrapToUtc, cancellationToken))
        {
            if (!optionSymbols.TryGetValue(trade.InstrumentName, out var instrument))
            {
                continue;
            }

            if (catchUpFromUtc is not null &&
                DateTimeOffset.FromUnixTimeMilliseconds(trade.Timestamp).UtcDateTime <= catchUpFromUtc.Value)
            {
                continue;
            }

            yield return new ExchangeTradeMessage(instrument, MapTrade(trade));
        }

        await foreach (var trade in apiClient.StreamFutureTradesAsync(_options.BaseAsset, bootstrapFromUtc, bootstrapToUtc, cancellationToken))
        {
            if (!futureSymbols.TryGetValue(trade.InstrumentName, out var instrument))
            {
                continue;
            }

            if (catchUpFromUtc is not null &&
                DateTimeOffset.FromUnixTimeMilliseconds(trade.Timestamp).UtcDateTime <= catchUpFromUtc.Value)
            {
                continue;
            }

            yield return new ExchangeTradeMessage(instrument, MapTrade(trade));
        }
    }

    public async IAsyncEnumerable<ExchangeTickerMessage> StreamTickersSnapshotAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var futureSymbols = instruments
            .Where(static x => x.Category.Equals("future", StringComparison.OrdinalIgnoreCase) || x.Category.Equals("perpetual", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var futureSummaries = await apiClient.GetFutureSummariesAsync(_options.BaseAsset, cancellationToken);
        var timestamp = DateTimeOffset.UtcNow;

        foreach (var summary in futureSummaries)
        {
            if (futureSymbols.TryGetValue(summary.InstrumentName, out var instrument))
            {
                yield return new ExchangeTickerMessage(instrument, DeribitApiClient.MapFutureTicker(summary, timestamp));
            }
        }
    }

    public async IAsyncEnumerable<ExchangeOptionMessage> StreamOptionChainSnapshotAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var optionSymbols = instruments
            .Where(static x => x.Category.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var snapshots = await apiClient.GetOptionChainSnapshotsAsync(cancellationToken);
        foreach (var snapshot in snapshots)
        {
            if (optionSymbols.TryGetValue(snapshot.Symbol, out var instrument))
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
        var instrumentMap = instruments.ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var tickerChannels = instruments
            .Where(static x => !x.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
            .Select(x => $"ticker.{x.Symbol}.{GetTickerInterval()}")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var socketGroups = tickerChannels
            .Chunk(Math.Max(1, _options.SubscriptionChunkSize))
            .Select((chunk, index) => new DeribitSocketSubscriptionGroup(
                $"tickers-{index + 1}",
                chunk))
            .Append(new DeribitSocketSubscriptionGroup(
                "trades",
                [ $"trades.future.{_options.BaseAsset}.100ms", $"trades.option.{_options.BaseAsset}.100ms" ]))
            .ToArray();

        using var groupCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var socketTasks = socketGroups
            .Select(group => RunSocketUntilDisconnectedAsync(group, instrumentMap, writer, groupCts.Token))
            .ToArray();

        try
        {
            await Task.WhenAny(socketTasks);
            await groupCts.CancelAsync();
            await Task.WhenAll(socketTasks);
        }
        catch (OperationCanceledException) when (groupCts.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            writer.TryComplete(exception);
            logger.LogWarning(exception, "Deribit socket group terminated with an exception.");
            throw;
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task RunSocketUntilDisconnectedAsync(
        DeribitSocketSubscriptionGroup group,
        IReadOnlyDictionary<string, InstrumentDefinition> instrumentMap,
        ChannelWriter<ExchangeDataMessage> writer,
        CancellationToken cancellationToken)
    {
        using var socket = new ClientWebSocket();
        socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
        await socket.ConnectAsync(new Uri(_options.WebSocketUrl), cancellationToken);

        await SendRequestAsync(socket, new
        {
            jsonrpc = "2.0",
            id = 1,
            method = "public/set_heartbeat",
            @params = new
            {
                interval = (int)Math.Max(10, _options.HeartbeatInterval.TotalSeconds)
            }
        }, cancellationToken);

        await SendRequestAsync(socket, new
        {
            jsonrpc = "2.0",
            id = 10,
            method = "public/subscribe",
            @params = new
            {
                channels = group.Channels
            }
        }, cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            string? message;
            try
            {
                message = await ReceiveMessageAsync(socket, cancellationToken);
            }
            catch (WebSocketException exception)
            {
                logger.LogWarning(exception, "Deribit websocket receive failed. Group={Group}, State={State}, CloseStatus={CloseStatus}, Description={Description}.",
                    group.Name,
                    socket.State,
                    socket.CloseStatus,
                    socket.CloseStatusDescription);
                break;
            }

            if (message is null)
            {
                logger.LogWarning("Deribit websocket closed. Group={Group}, State={State}, CloseStatus={CloseStatus}, Description={Description}.",
                    group.Name,
                    socket.State,
                    socket.CloseStatus,
                    socket.CloseStatusDescription);
                break;
            }

            using var document = JsonDocument.Parse(message);
            var root = document.RootElement;

            if (TryGetError(root, out var errorCode, out var errorMessage))
            {
                logger.LogWarning("Deribit websocket returned error response. Group={Group}, Code={Code}, Message={Message}.",
                    group.Name,
                    errorCode,
                    errorMessage);
                continue;
            }

            if (TryGetMethod(root, out var method))
            {
                if (string.Equals(method, "heartbeat", StringComparison.OrdinalIgnoreCase))
                {
                    if (TryGetHeartbeatType(root, out var heartbeatType) &&
                        string.Equals(heartbeatType, "test_request", StringComparison.OrdinalIgnoreCase))
                    {
                        await SendPublicTestAsync(socket, cancellationToken);
                    }

                    continue;
                }

                if (string.Equals(method, "test_request", StringComparison.OrdinalIgnoreCase))
                {
                    await SendPublicTestAsync(socket, cancellationToken);
                    continue;
                }
            }

            var payload = JsonSerializer.Deserialize<DeribitSubscriptionMessage>(message, JsonOptions);
            if (payload is null || !string.Equals(payload.Method, "subscription", StringComparison.OrdinalIgnoreCase) || payload.Params is null)
            {
                continue;
            }

            if (payload.Params.Channel.StartsWith("ticker.", StringComparison.OrdinalIgnoreCase))
            {
                var symbol = payload.Params.Data.TryGetProperty("instrument_name", out var instrumentProperty)
                    ? instrumentProperty.GetString()
                    : null;

                if (symbol is not null && instrumentMap.TryGetValue(symbol, out var instrument))
                {
                    var summary = payload.Params.Data.Deserialize<DeribitBookSummary>(JsonOptions);
                    if (summary is not null)
                    {
                        writer.TryWrite(new ExchangeTickerMessage(instrument, DeribitApiClient.MapFutureTicker(summary, DateTimeOffset.UtcNow)));
                    }
                }

                continue;
            }

            if (payload.Params.Channel.StartsWith("trades.", StringComparison.OrdinalIgnoreCase) &&
                payload.Params.Data.ValueKind == JsonValueKind.Array)
            {
                foreach (var tradeElement in payload.Params.Data.EnumerateArray())
                {
                    var trade = tradeElement.Deserialize<DeribitTrade>(JsonOptions);
                    if (trade is null || string.IsNullOrWhiteSpace(trade.InstrumentName))
                    {
                        continue;
                    }

                    if (instrumentMap.TryGetValue(trade.InstrumentName, out var instrument))
                    {
                        writer.TryWrite(new ExchangeTradeMessage(instrument, MapTrade(trade)));
                    }
                }
            }
        }

        if (socket.State == WebSocketState.Open)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "shutdown", CancellationToken.None);
        }
    }

    private static async Task SendRequestAsync(ClientWebSocket socket, object payload, CancellationToken cancellationToken)
    {
        var request = JsonSerializer.Serialize(payload);
        await socket.SendAsync(Encoding.UTF8.GetBytes(request), WebSocketMessageType.Text, true, cancellationToken);
    }

    private static Task SendPublicTestAsync(ClientWebSocket socket, CancellationToken cancellationToken) =>
        SendRequestAsync(socket, new
        {
            jsonrpc = "2.0",
            id = 2,
            method = "public/test",
            @params = new { }
        }, cancellationToken);

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

    private static bool TryGetMethod(JsonElement root, out string? method)
    {
        method = null;
        if (!root.TryGetProperty("method", out var methodProperty) || methodProperty.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        method = methodProperty.GetString();
        return !string.IsNullOrWhiteSpace(method);
    }

    private static bool TryGetError(JsonElement root, out int code, out string? message)
    {
        code = 0;
        message = null;

        if (!root.TryGetProperty("error", out var errorProperty) || errorProperty.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        if (errorProperty.TryGetProperty("code", out var codeProperty) && codeProperty.TryGetInt32(out var parsedCode))
        {
            code = parsedCode;
        }

        if (errorProperty.TryGetProperty("message", out var messageProperty) && messageProperty.ValueKind == JsonValueKind.String)
        {
            message = messageProperty.GetString();
        }

        return true;
    }

    private static bool TryGetHeartbeatType(JsonElement root, out string? heartbeatType)
    {
        heartbeatType = null;
        if (!root.TryGetProperty("params", out var paramsProperty) || paramsProperty.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        if (!paramsProperty.TryGetProperty("type", out var typeProperty) || typeProperty.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        heartbeatType = typeProperty.GetString();
        return !string.IsNullOrWhiteSpace(heartbeatType);
    }

    private static ExchangeTrade MapTrade(DeribitTrade trade) =>
        new()
        {
            TradeTime = DateTimeOffset.FromUnixTimeMilliseconds(trade.Timestamp),
            Symbol = trade.InstrumentName,
            Side = trade.Direction,
            Quantity = trade.Contracts ?? trade.Amount,
            Price = trade.Price,
            TradeId = trade.TradeId,
            IsBlockTrade = !string.IsNullOrWhiteSpace(trade.BlockTradeId),
            BlockTradeId = trade.BlockTradeId,
            IsRpiTrade = false,
            Sequence = trade.TradeSequence.ToString(System.Globalization.CultureInfo.InvariantCulture)
        };

    private string GetTickerInterval() =>
        string.Equals(_options.TickerInterval, "raw", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(_options.TickerInterval, "100ms", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(_options.TickerInterval, "agg2", StringComparison.OrdinalIgnoreCase)
            ? _options.TickerInterval
            : "agg2";

    private sealed record DeribitSocketSubscriptionGroup(string Name, string[] Channels);
}
