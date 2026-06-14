using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;
using CryptoCollector.Exchange.Deribit.Models;
using CryptoCollector.Exchange.Deribit.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Exchange.Deribit.Services;

public sealed class DeribitCollectorHostedService(
    DeribitApiClient apiClient,
    InstrumentCatalog instrumentCatalog,
    IMarketDataSink marketDataSink,
    IOptions<DeribitCollectorOptions> options,
    ILogger<DeribitCollectorHostedService> logger) : BackgroundService, IExchangeCollector
{
    private readonly DeribitCollectorOptions _options = options.Value;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public string Exchange => "deribit";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                try
                {
                    var instruments = await apiClient.GetTrackedInstrumentsAsync(_options.BaseAsset, _options.QuoteAsset, stoppingToken);
                    instrumentCatalog.Replace(Exchange, instruments);
                    logger.LogInformation("Loaded {Count} tracked Deribit instruments.", instruments.Count);
                }
                catch (Exception exception) when (instrumentCatalog.All.Any(x => x.Exchange.Equals(Exchange, StringComparison.OrdinalIgnoreCase)))
                {
                    var cachedCount = instrumentCatalog.All.Count(x => x.Exchange.Equals(Exchange, StringComparison.OrdinalIgnoreCase));
                    logger.LogWarning(exception, "Instrument refresh failed. Reusing cached Deribit catalog with {Count} instruments.", cachedCount);
                }

                try
                {
                    await BootstrapRestAsync(stoppingToken);
                }
                catch (Exception exception)
                {
                    logger.LogWarning(exception, "Deribit REST bootstrap failed. Continuing with websocket subscriptions.");
                }

                await RunSubscriptionsUntilRefreshAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Deribit collector cycle failed. Restarting after delay.");
                await Task.Delay(_options.ReconnectDelay, stoppingToken);
            }
        }
    }

    private async Task BootstrapRestAsync(CancellationToken cancellationToken)
    {
        var optionSymbols = instrumentCatalog.GetByCategory(Exchange, "option")
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var futureSymbols = instrumentCatalog.GetByCategory(Exchange, "future")
            .Concat(instrumentCatalog.GetByCategory(Exchange, "perpetual"))
            .DistinctBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        var optionSummaries = await apiClient.GetOptionSummariesAsync(_options.BaseAsset, cancellationToken);
        foreach (var summary in optionSummaries)
        {
            if (optionSymbols.TryGetValue(summary.InstrumentName, out var instrument))
            {
                marketDataSink.IngestTicker(instrument, ToJson(summary), DateTimeOffset.UtcNow);
            }
        }

        var futureSummaries = await apiClient.GetFutureSummariesAsync(_options.BaseAsset, cancellationToken);
        foreach (var summary in futureSummaries)
        {
            if (futureSymbols.TryGetValue(summary.InstrumentName, out var instrument))
            {
                marketDataSink.IngestTicker(instrument, ToJson(summary), DateTimeOffset.UtcNow);
            }
        }

        var optionTrades = await apiClient.GetRecentOptionTradesAsync(_options.BaseAsset, cancellationToken);
        foreach (var trade in optionTrades)
        {
            if (optionSymbols.TryGetValue(trade.InstrumentName, out var instrument))
            {
                marketDataSink.IngestTrade(instrument, MapTrade(trade));
            }
        }

        var futureTrades = await apiClient.GetRecentFutureTradesAsync(_options.BaseAsset, cancellationToken);
        foreach (var trade in futureTrades)
        {
            if (futureSymbols.TryGetValue(trade.InstrumentName, out var instrument))
            {
                marketDataSink.IngestTrade(instrument, MapTrade(trade));
            }
        }
    }

    private async Task RunSubscriptionsUntilRefreshAsync(CancellationToken cancellationToken)
    {
        using var socket = new ClientWebSocket();
        socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
        await socket.ConnectAsync(new Uri(_options.WebSocketUrl), cancellationToken);

        var instruments = instrumentCatalog.All
            .Where(x => x.Exchange.Equals(Exchange, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        var channels = instruments.Select(x => $"ticker.{x.Symbol}.100ms")
            .Concat([ $"trades.future.{_options.BaseAsset}.raw", $"trades.option.{_options.BaseAsset}.raw" ])
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var subscribePayload = JsonSerializer.Serialize(new
        {
            jsonrpc = "2.0",
            id = 1,
            method = "public/subscribe",
            @params = new
            {
                channels
            }
        });

        await socket.SendAsync(Encoding.UTF8.GetBytes(subscribePayload), WebSocketMessageType.Text, true, cancellationToken);

        using var refreshCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        refreshCts.CancelAfter(_options.InstrumentRefreshInterval);

        while (!refreshCts.IsCancellationRequested)
        {
            var message = await ReceiveMessageAsync(socket, refreshCts.Token);
            if (message is null)
            {
                break;
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

                if (symbol is not null && instrumentCatalog.TryGet(Exchange, symbol, out var instrument) && instrument is not null)
                {
                    marketDataSink.IngestTicker(instrument, payload.Params.Data, DateTimeOffset.UtcNow);
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

                    if (instrumentCatalog.TryGet(Exchange, trade.InstrumentName, out var instrument) && instrument is not null)
                    {
                        marketDataSink.IngestTrade(instrument, MapTrade(trade));
                    }
                }
            }
        }

        if (socket.State == WebSocketState.Open)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "refresh", CancellationToken.None);
        }
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

    private static JsonElement ToJson<T>(T value)
    {
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(value));
        return document.RootElement.Clone();
    }

    private static ExchangeTrade MapTrade(DeribitTrade trade) =>
        new()
        {
            TradeTime = trade.Timestamp,
            Symbol = trade.InstrumentName,
            Side = trade.Direction,
            Size = trade.Amount.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Price = trade.Price.ToString(System.Globalization.CultureInfo.InvariantCulture),
            TradeId = trade.TradeId,
            IsBlockTrade = !string.IsNullOrWhiteSpace(trade.BlockTradeId),
            IsRpiTrade = false,
            Sequence = trade.TradeSequence.ToString(System.Globalization.CultureInfo.InvariantCulture)
        };
}
