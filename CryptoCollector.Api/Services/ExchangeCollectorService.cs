using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;
using CryptoCollector.Api.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CryptoCollector.Api.Services;

public sealed class ExchangeCollectorService(
    IExchange exchange,
    DailyParquetStore store,
    IMarketDataSink marketDataSink,
    IFlushableMarketDataSink flushableMarketDataSink,
    BlockTradeAlertService blockTradeAlertService,
    ILogger<ExchangeCollectorService> logger) : BackgroundService
{
    private const int BootstrapTradeFlushBatchSize = 5_000;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        IReadOnlyList<InstrumentDefinition> instruments = [];

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                try
                {
                    instruments = await exchange.GetTrackedInstrumentsAsync(stoppingToken);
                    logger.LogInformation("Loaded {Count} tracked {Exchange} instruments.", instruments.Count, exchange.Name);
                }
                catch (Exception exception) when (instruments.Count > 0)
                {
                    logger.LogWarning(exception,
                        "Instrument refresh failed. Reusing cached {Exchange} catalog with {Count} instruments.",
                        exchange.Name,
                        instruments.Count);
                }

                if (instruments.Count == 0)
                {
                    logger.LogWarning("{Exchange} instrument catalog is empty. Reconnecting after {Delay}.", exchange.Name, exchange.ReconnectDelay);
                    await Task.Delay(exchange.ReconnectDelay, stoppingToken);
                    continue;
                }

                try
                {
                    var catchUpFromUtc = await store.GetLatestTimestampAsync<TradeRecord>(
                        exchange.Name,
                        DataSetNames.Trades,
                        stoppingToken);

                    if (catchUpFromUtc is not null)
                    {
                        logger.LogInformation(
                            "{Exchange} bootstrap catch-up starts from {CatchUpUtc:O}.",
                            exchange.Name,
                            catchUpFromUtc.Value);
                    }

                    await FlushCatchUpTradesAsync(instruments, catchUpFromUtc, stoppingToken);
                    await FlushTickerSnapshotAsync(instruments, stoppingToken);
                    await flushableMarketDataSink.FlushPendingAsync(stoppingToken);
                }
                catch (Exception exception)
                {
                    logger.LogWarning(exception, "{Exchange} REST bootstrap failed. Continuing with live subscriptions.", exchange.Name);
                }

                try
                {
                    await FlushOptionChainSnapshotAsync(instruments, stoppingToken);
                    await flushableMarketDataSink.FlushPendingAsync(stoppingToken);
                }
                catch (Exception exception)
                {
                    logger.LogWarning(exception, "{Exchange} initial option-chain snapshot failed. Continuing with live subscriptions.", exchange.Name);
                }

                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                var optionChainTask = RunOptionChainPollingLoopAsync(instruments, linkedCts.Token);
                Exception? streamFailure = null;

                try
                {
                    await foreach (var message in exchange.StreamAsync(instruments, linkedCts.Token))
                    {
                        FlushMessage(message);
                    }
                }
                catch (OperationCanceledException) when (linkedCts.IsCancellationRequested)
                {
                }
                catch (Exception exception)
                {
                    streamFailure = exception;
                }
                finally
                {
                    await linkedCts.CancelAsync();
                }

                try
                {
                    await optionChainTask;
                }
                catch (OperationCanceledException) when (linkedCts.IsCancellationRequested)
                {
                }

                if (streamFailure is not null)
                {
                    logger.LogError(streamFailure, "{Exchange} collector cycle failed. Restarting after delay.", exchange.Name);
                }
                else if (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogWarning("{Exchange} live stream disconnected. Reconnecting after {Delay}.", exchange.Name, exchange.ReconnectDelay);
                }

                if (!stoppingToken.IsCancellationRequested)
                {
                    await Task.Delay(exchange.ReconnectDelay, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task RunOptionChainPollingLoopAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(exchange.OptionChainSnapshotInterval);

        while (!cancellationToken.IsCancellationRequested && await timer.WaitForNextTickAsync(cancellationToken))
        {
            try
            {
                await FlushOptionChainSnapshotAsync(instruments, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "{Exchange} option-chain snapshot poll failed.", exchange.Name);
            }
        }
    }

    private void FlushMessage(ExchangeDataMessage message)
    {
        switch (message)
        {
            case ExchangeTradeMessage trade:
                marketDataSink.IngestTrade(trade.Instrument, trade.Trade);
                blockTradeAlertService.IngestLiveTrade(trade.Instrument, trade.Trade);
                break;
            case ExchangeTickerMessage ticker:
                marketDataSink.IngestTicker(ticker.Instrument, ticker.Ticker);
                break;
            case ExchangeOptionMessage option:
                marketDataSink.IngestOption(option.Instrument, option.OptionTicker);
                break;
            default:
                throw new InvalidOperationException($"Unsupported exchange message type: {message.GetType().Name}");
        }
    }

    private async Task FlushCatchUpTradesAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        DateTime? catchUpFromUtc,
        CancellationToken cancellationToken)
    {
        var processedCount = 0;

        await foreach (var trade in exchange.StreamTradesSinceAsync(instruments, catchUpFromUtc, cancellationToken))
        {
            marketDataSink.IngestTrade(trade.Instrument, trade.Trade);
            blockTradeAlertService.IngestRecoveredTrade(trade.Instrument, trade.Trade);
            processedCount++;

            if (processedCount % BootstrapTradeFlushBatchSize == 0)
            {
                await flushableMarketDataSink.FlushPendingAsync(cancellationToken);
            }
        }
    }

    private async Task FlushTickerSnapshotAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        CancellationToken cancellationToken)
    {
        await foreach (var ticker in exchange.StreamTickersSnapshotAsync(instruments, cancellationToken))
        {
            marketDataSink.IngestTicker(ticker.Instrument, ticker.Ticker);
        }
    }

    private async Task FlushOptionChainSnapshotAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        CancellationToken cancellationToken)
    {
        await foreach (var option in exchange.StreamOptionChainSnapshotAsync(instruments, cancellationToken))
        {
            marketDataSink.IngestOption(option.Instrument, option.OptionTicker);
        }
    }
}
