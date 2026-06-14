using Bybit.Net.Clients;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Services;
using CryptoCollector.Exchange.Bybit.Options;
using CryptoExchange.Net.Objects.Sockets;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Exchange.Bybit.Services;

public sealed class BybitCollectorHostedService(
    BybitApiClient apiClient,
    BybitSocketClient socketClient,
    InstrumentCatalog instrumentCatalog,
    IMarketDataSink marketDataSink,
    IOptions<BybitCollectorOptions> options,
    ILogger<BybitCollectorHostedService> logger) : BackgroundService, IExchangeCollector
{
    private readonly BybitCollectorOptions _options = options.Value;

    public string Exchange => "bybit";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                try
                {
                    var instruments = await apiClient.GetTrackedInstrumentsAsync(
                        _options.BaseAsset,
                        _options.QuoteAsset,
                        stoppingToken);

                    instrumentCatalog.Replace(Exchange, instruments);
                    logger.LogInformation("Loaded {Count} tracked Bybit instruments.", instruments.Count);
                }
                catch (Exception exception) when (instrumentCatalog.All.Count > 0)
                {
                    logger.LogWarning(exception, "Instrument refresh failed. Reusing cached catalog with {Count} instruments.", instrumentCatalog.All.Count);
                }

                try
                {
                    await BootstrapRestAsync(stoppingToken);
                }
                catch (Exception exception)
                {
                    logger.LogWarning(exception, "REST bootstrap failed. Continuing with websocket subscriptions.");
                }

                await RunSubscriptionsUntilRefreshAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Collector cycle failed. Restarting after delay.");
                await Task.Delay(_options.ReconnectDelay, stoppingToken);
            }
        }
    }

    private async Task RunSubscriptionsUntilRefreshAsync(CancellationToken stoppingToken)
    {
        var subscriptions = new List<UpdateSubscription>();
        var linearInstruments = instrumentCatalog.GetByCategory(Exchange, "linear");
        var optionInstruments = instrumentCatalog.GetByCategory(Exchange, "option");

        try
        {
            foreach (var chunk in linearInstruments.Select(static x => x.Symbol).Chunk(_options.LinearChunkSize))
            {
                subscriptions.Add(await SubscribeAsync(
                    socketClient.V5LinearApi.SubscribeToTickerUpdatesAsync(
                        chunk,
                        update =>
                        {
                            if (instrumentCatalog.TryGet(Exchange, update.Data.Symbol, out var instrument) && instrument is not null)
                            {
                                marketDataSink.IngestTicker(instrument, update.Data, DateTimeOffset.UtcNow);
                            }
                        },
                        stoppingToken)));

                subscriptions.Add(await SubscribeAsync(
                    socketClient.V5LinearApi.SubscribeToTradeUpdatesAsync(
                        chunk,
                        update =>
                        {
                            foreach (var trade in update.Data)
                            {
                                if (instrumentCatalog.TryGet(Exchange, trade.Symbol, out var instrument) && instrument is not null)
                                {
                                    marketDataSink.IngestTrade(instrument, trade);
                                }
                            }
                        },
                        stoppingToken)));
            }

            foreach (var chunk in optionInstruments.Select(static x => x.Symbol).Chunk(_options.OptionTickerChunkSize))
            {
                subscriptions.Add(await SubscribeAsync(
                    socketClient.V5OptionsApi.SubscribeToTickerUpdatesAsync(
                        chunk,
                        update =>
                        {
                            if (instrumentCatalog.TryGet(Exchange, update.Data.Symbol, out var instrument) && instrument is not null)
                            {
                                marketDataSink.IngestTicker(instrument, update.Data, DateTimeOffset.UtcNow);
                            }
                        },
                        stoppingToken)));
            }

            subscriptions.Add(await SubscribeAsync(
                socketClient.V5OptionsApi.SubscribeToTradeUpdatesAsync(
                    _options.BaseAsset,
                    update =>
                    {
                        foreach (var trade in update.Data)
                        {
                            if (instrumentCatalog.TryGet(Exchange, trade.Symbol, out var instrument) && instrument is not null)
                            {
                                marketDataSink.IngestTrade(instrument, trade);
                            }
                        }
                    },
                    stoppingToken)));

            await Task.Delay(_options.InstrumentRefreshInterval, stoppingToken);
        }
        finally
        {
            foreach (var subscription in subscriptions)
            {
                await socketClient.UnsubscribeAsync(subscription);
            }
        }
    }

    private async Task BootstrapRestAsync(CancellationToken cancellationToken)
    {
        await BootstrapLinearTickersAsync(cancellationToken);
        await BootstrapOptionTickersAsync(cancellationToken);
        await BootstrapLinearTradesAsync(cancellationToken);
        await BootstrapOptionTradesAsync(cancellationToken);
    }

    private async Task BootstrapLinearTickersAsync(CancellationToken cancellationToken)
    {
        var trackedSymbols = instrumentCatalog.GetByCategory(Exchange, "linear")
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        var tickers = await apiClient.GetLinearTickersAsync(_options.BaseAsset, cancellationToken);
        var timestamp = DateTimeOffset.UtcNow;

        foreach (var ticker in tickers)
        {
            if (trackedSymbols.TryGetValue(ticker.Symbol, out var instrument))
            {
                marketDataSink.IngestTicker(instrument, ticker, timestamp);
            }
        }
    }

    private async Task BootstrapOptionTickersAsync(CancellationToken cancellationToken)
    {
        var trackedSymbols = instrumentCatalog.GetByCategory(Exchange, "option")
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        var tickers = await apiClient.GetOptionTickersAsync(_options.BaseAsset, cancellationToken);
        var timestamp = DateTimeOffset.UtcNow;

        foreach (var ticker in tickers)
        {
            if (trackedSymbols.TryGetValue(ticker.Symbol, out var instrument))
            {
                marketDataSink.IngestTicker(instrument, ticker, timestamp);
            }
        }
    }

    private async Task BootstrapLinearTradesAsync(CancellationToken cancellationToken)
    {
        foreach (var instrument in instrumentCatalog.GetByCategory(Exchange, "linear"))
        {
            var trades = await apiClient.GetRecentLinearTradesAsync(instrument.Symbol, cancellationToken);
            foreach (var trade in trades)
            {
                marketDataSink.IngestTrade(instrument, trade);
            }
        }
    }

    private async Task BootstrapOptionTradesAsync(CancellationToken cancellationToken)
    {
        var trackedSymbols = instrumentCatalog.GetByCategory(Exchange, "option")
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        var trades = await apiClient.GetRecentOptionTradesAsync(_options.BaseAsset, cancellationToken);
        foreach (var trade in trades)
        {
            if (trackedSymbols.TryGetValue(trade.Symbol, out var instrument))
            {
                marketDataSink.IngestTrade(instrument, trade);
            }
        }
    }

    private static async Task<UpdateSubscription> SubscribeAsync(Task<CryptoExchange.Net.Objects.CallResult<UpdateSubscription>> task)
    {
        var result = await task;
        if (!result.Success || result.Data is null)
        {
            throw new InvalidOperationException($"Subscription failed: {result.Error}");
        }

        return result.Data;
    }
}
