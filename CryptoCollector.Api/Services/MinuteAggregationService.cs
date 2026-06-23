using System.Collections.Concurrent;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;
using CryptoCollector.Api.Models;
using CryptoCollector.Api.Options;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Api.Services;

public sealed class MinuteAggregationService(
    DailyParquetStore store,
    IOptions<AggregationOptions> aggregationOptions,
    IOptions<BlockTradesAlertOptions> blockTradeOptions,
    ILocalMessageBus localMessageBus,
    ILogger<MinuteAggregationService> logger) : BackgroundService, IMarketDataSink
{
    private static readonly TimeSpan OiRetentionWindow = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan RecentTradeIdWindow = TimeSpan.FromSeconds(10);
    private readonly ConcurrentDictionary<string, TradeRecord> _tradeRows = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, EnrichedBlockTradeRecord> _blockTradeRows = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTime> _latestPersistedTradeBySymbol = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, byte> _recentSeenTradeIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<SeenTradeMarker> _recentSeenTradeIdQueue = new();
    private readonly ConcurrentDictionary<string, TickerAccumulator> _tickerBars = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, OptionChainAccumulator> _optionBars = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SymbolOiTimeline> _oiTimelines = new(StringComparer.OrdinalIgnoreCase);
    private DateTime? _lastReportedMinute;
    private readonly decimal _minTradeQuantity = aggregationOptions.Value.MinTradeQuantity;
    private readonly decimal _minBlockTradeUsd = blockTradeOptions.Value.MinGroupUsd;
    private int _seenTradeIdsPreloaded;

    public void IngestTrade(InstrumentDefinition instrument, ExchangeTrade trade)
    {
        var timestamp = trade.TradeTime.UtcDateTime;
        var price = trade.Price;
        var quantity = trade.Quantity;

        if (quantity < _minTradeQuantity && !trade.IsBlockTrade)
        {
            return;
        }

        var symbolKey = $"{instrument.Exchange}|{instrument.Symbol}";
        var dedupeKey = $"{instrument.Exchange}|{instrument.Symbol}|{trade.TradeId}";

        if (_latestPersistedTradeBySymbol.TryGetValue(symbolKey, out var latestPersistedTimestamp))
        {
            if (timestamp < latestPersistedTimestamp)
            {
                return;
            }

            if (timestamp == latestPersistedTimestamp && !_recentSeenTradeIds.TryAdd(dedupeKey, 0))
            {
                return;
            }
        }
        else if (!_recentSeenTradeIds.TryAdd(dedupeKey, 0))
        {
            return;
        }

        _latestPersistedTradeBySymbol.AddOrUpdate(symbolKey, timestamp, (_, current) => current > timestamp ? current : timestamp);
        _recentSeenTradeIds.TryAdd(dedupeKey, 0);
        _recentSeenTradeIdQueue.Enqueue(new SeenTradeMarker(dedupeKey, timestamp));

        var key = $"{instrument.Exchange}|{instrument.Symbol}|{trade.TradeId}|{timestamp:O}";
        var tradeRecord = new TradeRecord
        {
            Exchange = instrument.Exchange,
            Symbol = instrument.Symbol,
            InstrumentType = instrument.InstrumentType,
            BaseAsset = instrument.BaseAsset,
            QuoteAsset = instrument.QuoteAsset,
            SettleAsset = instrument.SettleAsset,
            Date = timestamp,
            ExpiryUtc = instrument.ExpiryUtc,
            StrikePrice = instrument.StrikePrice,
            OptionSide = instrument.OptionSide,
            TradeId = trade.TradeId,
            Side = trade.Side,
            Contracts = trade.Contracts,
            Amount = trade.Amount,
            Price = price,
            MarkPrice = trade.MarkPrice,
            IndexPrice = trade.IndexPrice,
            Iv = trade.Iv,
            MarkIv = trade.MarkIv,
            TickDirection = trade.TickDirection,
            Quantity = quantity,
            Notional = price * quantity,
            IsBlockTrade = trade.IsBlockTrade,
            BlockTradeId = trade.BlockTradeId,
            BlockTradeLegCount = trade.BlockTradeLegCount,
            ComboId = trade.ComboId,
            ComboTradeId = trade.ComboTradeId,
            BlockRfqId = trade.BlockRfqId,
            Liquidation = trade.Liquidation,
            IsRpiTrade = trade.IsRpiTrade,
            Sequence = trade.Sequence
        };

        _tradeRows.TryAdd(key, tradeRecord);
        TryStageBlockTradeCandidate(instrument, trade);
    }

    public void IngestTicker(InstrumentDefinition instrument, ExchangeTicker ticker)
    {
        var minute = FloorToMinute(ticker.TimestampUtc.UtcDateTime);
        var key = $"{instrument.Exchange}|{instrument.Symbol}|{minute:O}";
        var tickerAccumulator = _tickerBars.GetOrAdd(key, _ => new TickerAccumulator(instrument, minute));
        tickerAccumulator.Apply(ticker);
        AppendOpenInterest(instrument, ticker.TimestampUtc.UtcDateTime, ticker.OpenInterest);
    }

    public void IngestOption(InstrumentDefinition instrument, ExchangeOptionTicker optionTicker)
    {
        var minute = FloorToMinute(optionTicker.TimestampUtc.UtcDateTime);
        var key = $"{instrument.Exchange}|{instrument.Symbol}|{minute:O}";
        var accumulator = _optionBars.GetOrAdd(key, _ => new OptionChainAccumulator(instrument, minute));
        accumulator.Apply(optionTicker);
        AppendOpenInterest(instrument, optionTicker.TimestampUtc.UtcDateTime, optionTicker.OpenInterest);
    }

    private void TryStageBlockTradeCandidate(InstrumentDefinition instrument, ExchangeTrade trade)
    {
        if (!BlockTradeCandidateResolver.TryResolveCandidate(
                instrument,
                trade,
                _minBlockTradeUsd,
                out var groupId,
                out var groupType,
                out var usdNotional))
        {
            return;
        }

        var timeline = _oiTimelines.GetOrAdd(
            BuildSymbolKey(instrument.Exchange, instrument.Symbol),
            _ => new SymbolOiTimeline(OiRetentionWindow));

        if (!timeline.TryResolveOrRegister(
                new PendingCandidate(
                    instrument,
                    trade,
                    groupId,
                    groupType,
                    usdNotional),
                out var resolved))
        {
            return;
        }

        StageResolvedBlockTrade(resolved);
    }

    private void AppendOpenInterest(InstrumentDefinition instrument, DateTime timestampUtc, decimal? openInterest)
    {
        if (openInterest is null)
        {
            return;
        }

        var timeline = _oiTimelines.GetOrAdd(
            BuildSymbolKey(instrument.Exchange, instrument.Symbol),
            _ => new SymbolOiTimeline(OiRetentionWindow));

        foreach (var resolved in timeline.Append(timestampUtc, openInterest.Value))
        {
            StageResolvedBlockTrade(resolved);
        }
    }

    private void StageResolvedBlockTrade(EnrichedBlockTradeRecord record)
    {
        var key = BuildTradeKey(record.Exchange, record.Symbol, record.TradeId, record.Date);
        if (!_blockTradeRows.TryAdd(key, record))
        {
            return;
        }

        localMessageBus.Publish(record);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await PreloadSeenTradeIdsAsync(stoppingToken);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));

        try
        {
            while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
            {
                await FlushAsync(includeCurrentMinute: false, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        DrainPendingBlockTradeCandidates();
        await FlushAsync(includeCurrentMinute: true, cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    public Task FlushPendingAsync(CancellationToken cancellationToken) =>
        FlushAsync(includeCurrentMinute: true, cancellationToken);

    private async Task PreloadSeenTradeIdsAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _seenTradeIdsPreloaded, 1) != 0)
        {
            return;
        }

        foreach (var exchange in new[] { "binance", "bybit", "deribit" })
        {
            var latestTimestamp = await store.GetLatestTimestampAsync<TradeRecord>(exchange, DataSetNames.Trades, cancellationToken);
            if (latestTimestamp is null)
            {
                continue;
            }

            var fromUtc = latestTimestamp.Value.Date;
            var toUtc = fromUtc.AddDays(1).AddTicks(-1);
            var rows = await store.QueryAsync<TradeRecord>(
                exchange,
                DataSetNames.Trades,
                fromUtc,
                toUtc,
                symbol: null,
                cancellationToken);

            var latestPerSymbol = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

            foreach (var row in rows)
            {
                var symbolKey = $"{row.Exchange}|{row.Symbol}";
                if (!latestPerSymbol.TryGetValue(symbolKey, out var current) || row.Date > current)
                {
                    latestPerSymbol[symbolKey] = row.Date;
                }
            }

            foreach (var entry in latestPerSymbol)
            {
                _latestPersistedTradeBySymbol[entry.Key] = entry.Value;
            }

            foreach (var row in rows)
            {
                var symbolKey = $"{row.Exchange}|{row.Symbol}";
                if (!latestPerSymbol.TryGetValue(symbolKey, out var latestForSymbol) ||
                    row.Date < latestForSymbol - RecentTradeIdWindow)
                {
                    continue;
                }

                var dedupeKey = $"{row.Exchange}|{row.Symbol}|{row.TradeId}";
                if (_recentSeenTradeIds.TryAdd(dedupeKey, 0))
                {
                    _recentSeenTradeIdQueue.Enqueue(new SeenTradeMarker(dedupeKey, row.Date));
                }
            }
        }
    }

    private async Task FlushAsync(bool includeCurrentMinute, CancellationToken cancellationToken)
    {
        var cutoff = includeCurrentMinute ? DateTime.MaxValue : FloorToMinute(DateTime.UtcNow);

        var tradeRows = DrainTrades(cutoff);
        var blockTradeRows = Drain(_blockTradeRows, cutoff);
        var tickerRows = Drain(_tickerBars, cutoff, static accumulator => accumulator.Build());
        var optionRows = Drain(_optionBars, cutoff, static accumulator => accumulator.Build());

        await AppendByExchangeAsync(DataSetNames.Trades, tradeRows, cancellationToken);
        await AppendByExchangeAsync(DataSetNames.BlockTrades, blockTradeRows, cancellationToken);
        await AppendByExchangeAsync(DataSetNames.Tickers, tickerRows, cancellationToken);
        await AppendByExchangeAsync(DataSetNames.OptionChain, optionRows, cancellationToken);

        if (tradeRows.Count + blockTradeRows.Count + tickerRows.Count + optionRows.Count > 0)
        {
            logger.LogInformation(
                "Flushed minute bars to Parquet. Trades: {TradeCount}, BlockTrades: {BlockTradeCount}, Tickers: {TickerCount}, OptionChain: {OptionCount}",
                tradeRows.Count,
                blockTradeRows.Count,
                tickerRows.Count,
                optionRows.Count);

            foreach (var latestTrade in tradeRows
                         .GroupBy(static x => $"{x.Exchange}|{x.Symbol}", StringComparer.OrdinalIgnoreCase)
                         .Select(static group => new
                         {
                             SymbolKey = group.Key,
                             LatestTimestamp = group.Max(static x => x.Date)
                         }))
            {
                _latestPersistedTradeBySymbol.AddOrUpdate(
                    latestTrade.SymbolKey,
                    latestTrade.LatestTimestamp,
                    (_, current) => current > latestTrade.LatestTimestamp ? current : latestTrade.LatestTimestamp);
            }
        }

        if (!includeCurrentMinute)
        {
            CleanupSeenTradeIds(cutoff - RecentTradeIdWindow);

            var reportedMinute = cutoff.AddMinutes(-1);
            if (_lastReportedMinute != reportedMinute)
            {
                _lastReportedMinute = reportedMinute;
                WriteExchangeStats(reportedMinute, tradeRows, tickerRows, optionRows);
            }
        }
    }

    private List<TradeRecord> DrainTrades(DateTime cutoff)
    {
        var rows = new List<TradeRecord>();

        foreach (var entry in _tradeRows)
        {
            if (entry.Value.Date >= cutoff)
            {
                continue;
            }

            if (_tradeRows.TryRemove(entry.Key, out var removed))
            {
                rows.Add(removed);
            }
        }

        return rows
            .OrderBy(static x => x.Date)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<TBar> Drain<TBar>(
        ConcurrentDictionary<string, TBar> source,
        DateTime cutoff)
        where TBar : class, ITimeSeriesRecord
    {
        var rows = new List<TBar>();

        foreach (var entry in source)
        {
            if (entry.Value.Date >= cutoff)
            {
                continue;
            }

            if (source.TryRemove(entry.Key, out var removed))
            {
                rows.Add(removed);
            }
        }

        return rows
            .OrderBy(static x => x.Date)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private void CleanupSeenTradeIds(DateTime thresholdUtc)
    {
        while (_recentSeenTradeIdQueue.TryPeek(out var marker) && marker.TimestampUtc < thresholdUtc)
        {
            if (!_recentSeenTradeIdQueue.TryDequeue(out marker))
            {
                continue;
            }

            _recentSeenTradeIds.TryRemove(marker.Key, out _);
        }
    }

    private void DrainPendingBlockTradeCandidates()
    {
        foreach (var timeline in _oiTimelines.Values)
        {
            foreach (var unresolved in timeline.DrainPending())
            {
                StageResolvedBlockTrade(unresolved);
            }
        }
    }

    private static List<TBar> Drain<TAccumulator, TBar>(
        ConcurrentDictionary<string, TAccumulator> source,
        DateTime cutoff,
        Func<TAccumulator, TBar> factory)
        where TAccumulator : MinuteAccumulatorBase
        where TBar : class, ITimeSeriesRecord
    {
        var rows = new List<TBar>();

        foreach (var entry in source)
        {
            if (entry.Value.Minute >= cutoff)
            {
                continue;
            }

            if (source.TryRemove(entry.Key, out var removed))
            {
                rows.Add(factory(removed));
            }
        }

        return rows;
    }

    private static DateTime FloorToMinute(DateTime value) =>
        new(value.Year, value.Month, value.Day, value.Hour, value.Minute, 0, DateTimeKind.Utc);

    private static string BuildSymbolKey(string exchange, string symbol) =>
        $"{exchange}|{symbol}";

    private static string BuildTradeKey(string exchange, string symbol, string tradeId, DateTime timestampUtc) =>
        $"{exchange}|{symbol}|{tradeId}|{timestampUtc:O}";

    private async Task AppendByExchangeAsync<T>(string dataSet, IReadOnlyCollection<T> rows, CancellationToken cancellationToken)
        where T : class, ITimeSeriesRecord
    {
        foreach (var exchangeGroup in rows.GroupBy(static x => x.Exchange, StringComparer.OrdinalIgnoreCase))
        {
            await store.AppendAsync(exchangeGroup.Key, dataSet, exchangeGroup.ToArray(), cancellationToken);
        }
    }

    private void WriteExchangeStats(
        DateTime reportedMinute,
        IReadOnlyCollection<TradeRecord> tradeRows,
        IReadOnlyCollection<TickerMinuteBar> tickerRows,
        IReadOnlyCollection<OptionChainMinuteBar> optionRows)
    {
        var tradeCounts = CountByExchange(tradeRows);
        var tickerCounts = CountByExchange(tickerRows);
        var optionCounts = CountByExchange(optionRows);
        var pendingTradeCounts = CountByExchange(_tradeRows.Values);
        var pendingTickerCounts = CountByExchange(_tickerBars.Values);
        var pendingOptionCounts = CountByExchange(_optionBars.Values);
        var seenTradeCounts = CountSeenTradeIdsByExchange();
        var tradeTypeCounts = CountByExchangeAndInstrumentType(tradeRows);
        var tickerTypeCounts = CountByExchangeAndInstrumentType(tickerRows);
        var optionTypeCounts = CountByExchangeAndInstrumentType(optionRows);
        var pendingTradeTypeCounts = CountByExchangeAndInstrumentType(_tradeRows.Values);
        var pendingTickerTypeCounts = CountByExchangeAndInstrumentType(_tickerBars.Values);
        var pendingOptionTypeCounts = CountByExchangeAndInstrumentType(_optionBars.Values);

        var exchanges = tradeCounts.Keys
            .Concat(tickerCounts.Keys)
            .Concat(optionCounts.Keys)
            .Concat(pendingTradeCounts.Keys)
            .Concat(pendingTickerCounts.Keys)
            .Concat(pendingOptionCounts.Keys)
            .Concat(seenTradeCounts.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        foreach (var exchange in exchanges)
        {
            Console.WriteLine(
                $"[{DateTimeOffset.UtcNow:O}] exchange={exchange} minute={reportedMinute:yyyy-MM-dd HH:mm}Z trades={tradeCounts.GetValueOrDefault(exchange)} tickers={tickerCounts.GetValueOrDefault(exchange)} optionChain={optionCounts.GetValueOrDefault(exchange)} pendingTrades={pendingTradeCounts.GetValueOrDefault(exchange)} pendingTickers={pendingTickerCounts.GetValueOrDefault(exchange)} pendingOptionChain={pendingOptionCounts.GetValueOrDefault(exchange)} seenTradeIds={seenTradeCounts.GetValueOrDefault(exchange)}");
            WriteInstrumentTypeBreakdown(exchange, "trades", tradeTypeCounts);
            WriteInstrumentTypeBreakdown(exchange, "tickers", tickerTypeCounts);
            WriteInstrumentTypeBreakdown(exchange, "optionChain", optionTypeCounts);
            WriteInstrumentTypeBreakdown(exchange, "pendingTrades", pendingTradeTypeCounts);
            WriteInstrumentTypeBreakdown(exchange, "pendingTickers", pendingTickerTypeCounts);
            WriteInstrumentTypeBreakdown(exchange, "pendingOptionChain", pendingOptionTypeCounts);
        }
    }

    private static Dictionary<string, int> CountByExchange<T>(IEnumerable<T> rows) where T : class
    {
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var row in rows)
        {
            var exchange = row switch
            {
                ITimeSeriesRecord timeSeriesRecord => timeSeriesRecord.Exchange,
                MinuteAccumulatorBase accumulator => accumulator.Exchange,
                _ => string.Empty
            };

            if (string.IsNullOrWhiteSpace(exchange))
            {
                continue;
            }

            counts.TryGetValue(exchange, out var count);
            counts[exchange] = count + 1;
        }

        return counts;
    }

    private Dictionary<string, int> CountSeenTradeIdsByExchange()
    {
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var seenTradeKey in _recentSeenTradeIds.Keys)
        {
            var separatorIndex = seenTradeKey.IndexOf('|');
            if (separatorIndex <= 0)
            {
                continue;
            }

            var exchange = seenTradeKey[..separatorIndex];
            counts.TryGetValue(exchange, out var count);
            counts[exchange] = count + 1;
        }

        return counts;
    }

    private static Dictionary<ExchangeInstrumentTypeKey, int> CountByExchangeAndInstrumentType<T>(IEnumerable<T> rows) where T : class
    {
        var counts = new Dictionary<ExchangeInstrumentTypeKey, int>();

        foreach (var row in rows)
        {
            var key = row switch
            {
                TradeRecord tradeRecord => new ExchangeInstrumentTypeKey(tradeRecord.Exchange, tradeRecord.InstrumentType),
                TickerMinuteBar tickerMinuteBar => new ExchangeInstrumentTypeKey(tickerMinuteBar.Exchange, tickerMinuteBar.InstrumentType),
                OptionChainMinuteBar optionChainMinuteBar => new ExchangeInstrumentTypeKey(optionChainMinuteBar.Exchange, optionChainMinuteBar.InstrumentType),
                MinuteAccumulatorBase accumulator => new ExchangeInstrumentTypeKey(accumulator.Exchange, accumulator.InstrumentType),
                _ => default
            };

            if (string.IsNullOrWhiteSpace(key.Exchange))
            {
                continue;
            }

            counts.TryGetValue(key, out var count);
            counts[key] = count + 1;
        }

        return counts;
    }

    private static void WriteInstrumentTypeBreakdown(
        string exchange,
        string metricName,
        IReadOnlyDictionary<ExchangeInstrumentTypeKey, int> counts)
    {
        var typedCounts = counts
            .Where(x => x.Key.Exchange.Equals(exchange, StringComparison.OrdinalIgnoreCase))
            .OrderBy(x => x.Key.InstrumentType)
            .ToArray();

        if (typedCounts.Length == 0)
        {
            return;
        }

        var breakdown = string.Join(", ", typedCounts.Select(x => $"{x.Key.InstrumentType}={x.Value}"));
        Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] exchange={exchange} {metricName} ({breakdown})");
    }

    private sealed record SeenTradeMarker(string Key, DateTime TimestampUtc);
    private readonly record struct ExchangeInstrumentTypeKey(string Exchange, InstrumentType InstrumentType);

    private abstract class MinuteAccumulatorBase(InstrumentDefinition instrument, DateTime minute)
    {
        protected InstrumentDefinition Instrument { get; } = instrument;
        public string Exchange => Instrument.Exchange;
        public InstrumentType InstrumentType => Instrument.InstrumentType;
        public DateTime Minute { get; } = minute;
    }

    private sealed class TickerAccumulator(InstrumentDefinition instrument, DateTime minute) : MinuteAccumulatorBase(instrument, minute)
    {
        private readonly Lock _gate = new();
        private decimal? _lastPrice;
        private decimal? _markPrice;
        private decimal? _indexPrice;
        private decimal? _bidPrice;
        private decimal? _bidSize;
        private decimal? _askPrice;
        private decimal? _askSize;
        private decimal? _openInterest;
        private decimal? _openInterestValue;
        private decimal? _volume24h;
        private decimal? _turnover24h;
        private decimal? _fundingRate;
        private decimal? _basisRate;
        private decimal? _basisRateYear;
        private DateTime? _deliveryUtc;
        private DateTime? _nextFundingUtc;
        private DateTime _lastUpdateUtc = minute;

        public void Apply(ExchangeTicker ticker)
        {
            lock (_gate)
            {
                _lastPrice = ticker.LastPrice ?? _lastPrice;
                _markPrice = ticker.MarkPrice ?? _markPrice;
                _indexPrice = ticker.IndexPrice ?? _indexPrice;
                _bidPrice = ticker.BidPrice ?? _bidPrice;
                _bidSize = ticker.BidSize ?? _bidSize;
                _askPrice = ticker.AskPrice ?? _askPrice;
                _askSize = ticker.AskSize ?? _askSize;
                _openInterest = ticker.OpenInterest ?? _openInterest;
                _openInterestValue = ticker.OpenInterestValue ?? _openInterestValue;
                _volume24h = ticker.Volume24h ?? _volume24h;
                _turnover24h = ticker.Turnover24h ?? _turnover24h;
                _fundingRate = ticker.FundingRate ?? _fundingRate;
                _basisRate = ticker.BasisRate ?? _basisRate;
                _basisRateYear = ticker.BasisRateYear ?? _basisRateYear;
                _deliveryUtc = ticker.DeliveryUtc ?? _deliveryUtc;
                _nextFundingUtc = ticker.NextFundingTimeUtc ?? _nextFundingUtc;
                _lastUpdateUtc = ticker.TimestampUtc.UtcDateTime;
            }
        }

        public TickerMinuteBar Build()
        {
            lock (_gate)
            {
                return new TickerMinuteBar
                {
                    Exchange = Instrument.Exchange,
                    Symbol = Instrument.Symbol,
                    InstrumentType = Instrument.InstrumentType,
                    BaseAsset = Instrument.BaseAsset,
                    QuoteAsset = Instrument.QuoteAsset,
                    SettleAsset = Instrument.SettleAsset,
                    Date = Minute,
                    ExpiryUtc = Instrument.ExpiryUtc,
                    LastPrice = _lastPrice,
                    MarkPrice = _markPrice,
                    IndexPrice = _indexPrice,
                    BidPrice = _bidPrice,
                    BidSize = _bidSize,
                    AskPrice = _askPrice,
                    AskSize = _askSize,
                    OpenInterest = _openInterest,
                    OpenInterestValue = _openInterestValue,
                    Volume24h = _volume24h,
                    Turnover24h = _turnover24h,
                    FundingRate = _fundingRate,
                    BasisRate = _basisRate,
                    BasisRateYear = _basisRateYear,
                    DeliveryUtc = _deliveryUtc,
                    NextFundingTimeUtc = _nextFundingUtc,
                    LastUpdateUtc = _lastUpdateUtc
                };
            }
        }
    }

    private sealed class OptionChainAccumulator(InstrumentDefinition instrument, DateTime minute) : MinuteAccumulatorBase(instrument, minute)
    {
        private readonly Lock _gate = new();
        private decimal? _bidPrice;
        private decimal? _bidSize;
        private decimal? _bidIv;
        private decimal? _askPrice;
        private decimal? _askSize;
        private decimal? _askIv;
        private decimal? _lastPrice;
        private decimal? _markPrice;
        private decimal? _indexPrice;
        private decimal? _markIv;
        private decimal? _underlyingPrice;
        private decimal? _openInterest;
        private decimal? _volume24h;
        private decimal? _turnover24h;
        private decimal? _totalVolume;
        private decimal? _totalTurnover;
        private decimal? _delta;
        private decimal? _gamma;
        private decimal? _vega;
        private decimal? _theta;
        private decimal? _change24h;
        private DateTime _lastUpdateUtc = minute;

        public void Apply(ExchangeOptionTicker optionTicker)
        {
            lock (_gate)
            {
                _bidPrice = optionTicker.BidPrice ?? _bidPrice;
                _bidSize = optionTicker.BidSize ?? _bidSize;
                _bidIv = optionTicker.BidIv ?? _bidIv;
                _askPrice = optionTicker.AskPrice ?? _askPrice;
                _askSize = optionTicker.AskSize ?? _askSize;
                _askIv = optionTicker.AskIv ?? _askIv;
                _lastPrice = optionTicker.LastPrice ?? _lastPrice;
                _markPrice = optionTicker.MarkPrice ?? _markPrice;
                _indexPrice = optionTicker.IndexPrice ?? _indexPrice;
                _markIv = optionTicker.MarkIv ?? _markIv;
                _underlyingPrice = optionTicker.UnderlyingPrice ?? _underlyingPrice;
                _openInterest = optionTicker.OpenInterest ?? _openInterest;
                _volume24h = optionTicker.Volume24h ?? _volume24h;
                _turnover24h = optionTicker.Turnover24h ?? _turnover24h;
                _totalVolume = optionTicker.TotalVolume ?? _totalVolume;
                _totalTurnover = optionTicker.TotalTurnover ?? _totalTurnover;
                _delta = optionTicker.Delta ?? _delta;
                _gamma = optionTicker.Gamma ?? _gamma;
                _vega = optionTicker.Vega ?? _vega;
                _theta = optionTicker.Theta ?? _theta;
                _change24h = optionTicker.Change24h ?? _change24h;
                _lastUpdateUtc = optionTicker.TimestampUtc.UtcDateTime;
            }
        }

        public OptionChainMinuteBar Build()
        {
            lock (_gate)
            {
                return new OptionChainMinuteBar
                {
                    Exchange = Instrument.Exchange,
                    Symbol = Instrument.Symbol,
                    InstrumentType = Instrument.InstrumentType,
                    BaseAsset = Instrument.BaseAsset,
                    QuoteAsset = Instrument.QuoteAsset,
                    SettleAsset = Instrument.SettleAsset,
                    Date = Minute,
                    ExpiryUtc = Instrument.ExpiryUtc,
                    StrikePrice = Instrument.StrikePrice,
                    OptionSide = Instrument.OptionSide,
                    BidPrice = _bidPrice,
                    BidSize = _bidSize,
                    BidIv = _bidIv,
                    AskPrice = _askPrice,
                    AskSize = _askSize,
                    AskIv = _askIv,
                    LastPrice = _lastPrice,
                    MarkPrice = _markPrice,
                    IndexPrice = _indexPrice,
                    MarkIv = _markIv,
                    UnderlyingPrice = _underlyingPrice,
                    OpenInterest = _openInterest,
                    Volume24h = _volume24h,
                    Turnover24h = _turnover24h,
                    TotalVolume = _totalVolume,
                    TotalTurnover = _totalTurnover,
                    Delta = _delta,
                    Gamma = _gamma,
                    Vega = _vega,
                    Theta = _theta,
                    Change24h = _change24h,
                    LastUpdateUtc = _lastUpdateUtc
                };
            }
        }
    }

    private sealed record PendingCandidate(
        InstrumentDefinition Instrument,
        ExchangeTrade Trade,
        string GroupId,
        string GroupType,
        decimal UsdNotional)
    {
        public DateTime TradeTimeUtc => Trade.TradeTime.UtcDateTime;

        public EnrichedBlockTradeRecord ToResolved(decimal? preTradeOpenInterest, decimal postTradeOpenInterest)
        {
            return new()
            {
                Exchange = Instrument.Exchange,
                Symbol = Instrument.Symbol,
                InstrumentType = Instrument.InstrumentType,
                BaseAsset = Instrument.BaseAsset,
                QuoteAsset = Instrument.QuoteAsset,
                SettleAsset = Instrument.SettleAsset,
                Date = TradeTimeUtc,
                ExpiryUtc = Instrument.ExpiryUtc,
                StrikePrice = Instrument.StrikePrice,
                OptionSide = Instrument.OptionSide,
                TradeId = Trade.TradeId,
                Side = Trade.Side,
                Quantity = Trade.Quantity,
                Contracts = Trade.Contracts,
                Amount = Trade.Amount,
                Price = Trade.Price,
                MarkPrice = Trade.MarkPrice,
                IndexPrice = Trade.IndexPrice,
                Iv = Trade.Iv,
                MarkIv = Trade.MarkIv,
                Notional = Trade.Price * Trade.Quantity,
                UsdNotional = UsdNotional,
                IsBlockTrade = Trade.IsBlockTrade,
                BlockTradeId = Trade.BlockTradeId,
                BlockTradeLegCount = Trade.BlockTradeLegCount,
                ComboId = Trade.ComboId,
                ComboTradeId = Trade.ComboTradeId,
                BlockRfqId = Trade.BlockRfqId,
                Liquidation = Trade.Liquidation,
                IsRpiTrade = Trade.IsRpiTrade,
                Sequence = Trade.Sequence,
                GroupId = GroupId,
                GroupType = GroupType,
                PreTradeOpenInterest = preTradeOpenInterest,
                PostTradeOpenInterest = postTradeOpenInterest
            };
        }

        public EnrichedBlockTradeRecord ToUnresolved(decimal? preTradeOpenInterest) =>
            new()
            {
                Exchange = Instrument.Exchange,
                Symbol = Instrument.Symbol,
                InstrumentType = Instrument.InstrumentType,
                BaseAsset = Instrument.BaseAsset,
                QuoteAsset = Instrument.QuoteAsset,
                SettleAsset = Instrument.SettleAsset,
                Date = TradeTimeUtc,
                ExpiryUtc = Instrument.ExpiryUtc,
                StrikePrice = Instrument.StrikePrice,
                OptionSide = Instrument.OptionSide,
                TradeId = Trade.TradeId,
                Side = Trade.Side,
                Quantity = Trade.Quantity,
                Contracts = Trade.Contracts,
                Amount = Trade.Amount,
                Price = Trade.Price,
                MarkPrice = Trade.MarkPrice,
                IndexPrice = Trade.IndexPrice,
                Iv = Trade.Iv,
                MarkIv = Trade.MarkIv,
                Notional = Trade.Price * Trade.Quantity,
                UsdNotional = UsdNotional,
                IsBlockTrade = Trade.IsBlockTrade,
                BlockTradeId = Trade.BlockTradeId,
                BlockTradeLegCount = Trade.BlockTradeLegCount,
                ComboId = Trade.ComboId,
                ComboTradeId = Trade.ComboTradeId,
                BlockRfqId = Trade.BlockRfqId,
                Liquidation = Trade.Liquidation,
                IsRpiTrade = Trade.IsRpiTrade,
                Sequence = Trade.Sequence,
                GroupId = GroupId,
                GroupType = GroupType,
                PreTradeOpenInterest = preTradeOpenInterest,
                PostTradeOpenInterest = null
            };
    }

    private sealed class SymbolOiTimeline(TimeSpan retentionWindow)
    {
        private readonly Lock _gate = new();
        private readonly List<OiPoint> _points = [];
        private readonly List<PendingWithPre> _pending = [];

        public bool TryResolveOrRegister(PendingCandidate candidate, out EnrichedBlockTradeRecord resolved)
        {
            lock (_gate)
            {
                var preTradeOpenInterest = ResolvePreTradeOpenInterest(candidate.TradeTimeUtc);
                if (TryResolvePostTradeOpenInterest(candidate.TradeTimeUtc, out var postTradeOpenInterest))
                {
                    resolved = candidate.ToResolved(preTradeOpenInterest, postTradeOpenInterest);
                    return true;
                }

                _pending.Add(new PendingWithPre(candidate, preTradeOpenInterest));
                resolved = null!;
                return false;
            }
        }

        public IReadOnlyList<EnrichedBlockTradeRecord> Append(DateTime timestampUtc, decimal openInterest)
        {
            lock (_gate)
            {
                if (_points.Count == 0)
                {
                    _points.Add(new OiPoint(timestampUtc, openInterest));
                }
                else
                {
                    var last = _points[^1];
                    if (last.OpenInterest != openInterest)
                    {
                        _points.Add(new OiPoint(timestampUtc, openInterest));
                    }
                }

                var resolved = new List<EnrichedBlockTradeRecord>();
                PruneExpired(timestampUtc - retentionWindow, resolved);

                if (_pending.Count == 0)
                {
                    return resolved;
                }

                for (var i = _pending.Count - 1; i >= 0; i--)
                {
                    var pending = _pending[i];
                    if (!TryResolvePostTradeOpenInterest(pending.Candidate.TradeTimeUtc, out var postTradeOpenInterest))
                    {
                        continue;
                    }

                    resolved.Add(pending.Candidate.ToResolved(pending.PreTradeOpenInterest, postTradeOpenInterest));
                    _pending.RemoveAt(i);
                }

                return resolved;
            }
        }

        public IReadOnlyList<EnrichedBlockTradeRecord> DrainPending()
        {
            lock (_gate)
            {
                if (_pending.Count == 0)
                {
                    return [];
                }

                var unresolved = new List<EnrichedBlockTradeRecord>(_pending.Count);
                foreach (var pending in _pending)
                {
                    unresolved.Add(pending.Candidate.ToUnresolved(pending.PreTradeOpenInterest));
                }

                _pending.Clear();
                return unresolved;
            }
        }

        private decimal? ResolvePreTradeOpenInterest(DateTime tradeTimeUtc)
        {
            for (var i = _points.Count - 1; i >= 0; i--)
            {
                if (_points[i].TimestampUtc <= tradeTimeUtc)
                {
                    return _points[i].OpenInterest;
                }
            }

            return null;
        }

        private bool TryResolvePostTradeOpenInterest(DateTime tradeTimeUtc, out decimal postTradeOpenInterest)
        {
            foreach (var point in _points)
            {
                if (point.TimestampUtc > tradeTimeUtc)
                {
                    postTradeOpenInterest = point.OpenInterest;
                    return true;
                }
            }

            postTradeOpenInterest = default;
            return false;
        }

        private void PruneExpired(DateTime thresholdUtc, List<EnrichedBlockTradeRecord> expired)
        {
            var oldestPendingTradeTimeUtc = _pending.Count == 0
                ? DateTime.MaxValue
                : _pending.Min(static x => x.Candidate.TradeTimeUtc);

            while (_points.Count > 0 &&
                   _points[0].TimestampUtc < thresholdUtc &&
                   _points[0].TimestampUtc < oldestPendingTradeTimeUtc)
            {
                _points.RemoveAt(0);
            }

            for (var i = _pending.Count - 1; i >= 0; i--)
            {
                if (_pending[i].Candidate.TradeTimeUtc < thresholdUtc)
                {
                    expired.Add(_pending[i].Candidate.ToUnresolved(_pending[i].PreTradeOpenInterest));
                    _pending.RemoveAt(i);
                }
            }
        }
    }

    private readonly record struct OiPoint(DateTime TimestampUtc, decimal OpenInterest);
    private sealed record PendingWithPre(PendingCandidate Candidate, decimal? PreTradeOpenInterest);
}
