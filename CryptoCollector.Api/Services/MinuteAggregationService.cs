using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using Bybit.Net.Objects.Models.V5;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;
using CryptoCollector.Api.Models;
using CryptoCollector.Exchange.Bybit.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Api.Services;

public sealed class MinuteAggregationService(
    DailyParquetStore store,
    IOptions<BybitCollectorOptions> options,
    ILogger<MinuteAggregationService> logger) : BackgroundService, IMarketDataSink
{
    private readonly ConcurrentDictionary<string, TradeRecord> _tradeRows = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTime> _seenTradeIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TickerAccumulator> _tickerBars = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, OptionChainAccumulator> _optionBars = new(StringComparer.OrdinalIgnoreCase);
    private DateTime? _lastReportedMinute;
    private readonly decimal _minTradeQuantity = options.Value.MinTradeQuantity;

    public void IngestTrade(InstrumentDefinition instrument, ExchangeTrade trade)
    {
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(trade.TradeTime).UtcDateTime;
        if (!decimal.TryParse(trade.Price, NumberStyles.Any, CultureInfo.InvariantCulture, out var price) ||
            !decimal.TryParse(trade.Size, NumberStyles.Any, CultureInfo.InvariantCulture, out var quantity))
        {
            return;
        }

        if (quantity < _minTradeQuantity && !trade.IsBlockTrade)
        {
            return;
        }

        var dedupeKey = $"{instrument.Symbol}|{trade.TradeId}";
        if (!_seenTradeIds.TryAdd(dedupeKey, timestamp))
        {
            return;
        }

        var key = $"{instrument.Symbol}|{trade.TradeId}|{timestamp:O}";
        _tradeRows.TryAdd(key, new TradeRecord
        {
            Exchange = instrument.Exchange,
            Symbol = instrument.Symbol,
            MarketType = instrument.MarketType,
            BaseAsset = instrument.BaseAsset,
            QuoteAsset = instrument.QuoteAsset,
            SettleAsset = instrument.SettleAsset,
            Date = timestamp,
            ExpiryUtc = instrument.ExpiryUtc,
            StrikePrice = instrument.StrikePrice,
            OptionSide = instrument.OptionSide,
            TradeId = trade.TradeId,
            Side = trade.Side,
            Price = price,
            Quantity = quantity,
            Notional = price * quantity,
            IsBlockTrade = trade.IsBlockTrade,
            IsRpiTrade = trade.IsRpiTrade,
            Sequence = trade.Sequence
        });
    }

    public void IngestTrade(InstrumentDefinition instrument, BybitTrade trade)
    {
        IngestTrade(instrument, new ExchangeTrade
        {
            TradeTime = new DateTimeOffset(trade.Timestamp).ToUnixTimeMilliseconds(),
            Symbol = trade.Symbol,
            Side = trade.Side.ToString(),
            Size = trade.Quantity.ToString(CultureInfo.InvariantCulture),
            Price = trade.Price.ToString(CultureInfo.InvariantCulture),
            TradeId = trade.TradeId,
            IsBlockTrade = trade.IsBlockTrade ?? false,
            IsRpiTrade = trade.IsRpiTrade ?? false,
            Sequence = (trade.Sequence ?? 0).ToString(CultureInfo.InvariantCulture)
        });
    }

    public void IngestTrade(InstrumentDefinition instrument, BybitTradeHistory trade)
    {
        IngestTrade(instrument, new ExchangeTrade
        {
            TradeTime = new DateTimeOffset(trade.Timestamp).ToUnixTimeMilliseconds(),
            Symbol = trade.Symbol,
            Side = trade.Side.ToString(),
            Size = trade.Quantity.ToString(CultureInfo.InvariantCulture),
            Price = trade.Price.ToString(CultureInfo.InvariantCulture),
            TradeId = trade.TradeId,
            IsBlockTrade = trade.IsBlockTrade,
            IsRpiTrade = trade.IsRpiTrade ?? false,
            Sequence = (trade.Sequence ?? 0).ToString(CultureInfo.InvariantCulture)
        });
    }

    public void IngestTrade(InstrumentDefinition instrument, BybitOptionTrade trade)
    {
        IngestTrade(instrument, new ExchangeTrade
        {
            TradeTime = new DateTimeOffset(trade.Timestamp).ToUnixTimeMilliseconds(),
            Symbol = trade.Symbol,
            Side = trade.Side.ToString(),
            Size = trade.Quantity.ToString(CultureInfo.InvariantCulture),
            Price = trade.Price.ToString(CultureInfo.InvariantCulture),
            TradeId = trade.TradeId,
            IsBlockTrade = trade.IsBlockTrade ?? false,
            IsRpiTrade = trade.IsRpiTrade ?? false,
            Sequence = (trade.Sequence ?? 0).ToString(CultureInfo.InvariantCulture)
        });
    }

    public void IngestTicker(InstrumentDefinition instrument, JsonElement payload, DateTimeOffset eventTimestamp)
    {
        var minute = FloorToMinute(eventTimestamp.UtcDateTime);
        var key = $"{instrument.Symbol}|{minute:O}";

        if (instrument.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
        {
            var accumulator = _optionBars.GetOrAdd(key, _ => new OptionChainAccumulator(instrument, minute));
            accumulator.Apply(payload, eventTimestamp.UtcDateTime);
            return;
        }

        var tickerAccumulator = _tickerBars.GetOrAdd(key, _ => new TickerAccumulator(instrument, minute));
        tickerAccumulator.Apply(payload, eventTimestamp.UtcDateTime);
    }

    public void IngestTicker(InstrumentDefinition instrument, BybitLinearTickerUpdate payload, DateTimeOffset eventTimestamp) =>
        IngestTickerAsJson(instrument, payload, eventTimestamp);

    public void IngestTicker(InstrumentDefinition instrument, BybitLinearInverseTicker payload, DateTimeOffset eventTimestamp) =>
        IngestTickerAsJson(instrument, payload, eventTimestamp);

    public void IngestTicker(InstrumentDefinition instrument, BybitOptionTickerUpdate payload, DateTimeOffset eventTimestamp) =>
        IngestTickerAsJson(instrument, payload, eventTimestamp);

    public void IngestTicker(InstrumentDefinition instrument, BybitOptionTicker payload, DateTimeOffset eventTimestamp) =>
        IngestTickerAsJson(instrument, payload, eventTimestamp);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
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
        await FlushAsync(includeCurrentMinute: true, cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    private async Task FlushAsync(bool includeCurrentMinute, CancellationToken cancellationToken)
    {
        var cutoff = includeCurrentMinute ? DateTime.MaxValue : FloorToMinute(DateTime.UtcNow);

        var tradeRows = DrainTrades(cutoff);
        var tickerRows = Drain(_tickerBars, cutoff, static accumulator => accumulator.Build());
        var optionRows = Drain(_optionBars, cutoff, static accumulator => accumulator.Build());

        if (tradeRows.Count > 0)
        {
            await store.AppendAsync("bybit", DataSetNames.Trades, tradeRows, cancellationToken);
        }

        if (tickerRows.Count > 0)
        {
            await store.AppendAsync("bybit", DataSetNames.Tickers, tickerRows, cancellationToken);
        }

        if (optionRows.Count > 0)
        {
            await store.AppendAsync("bybit", DataSetNames.OptionChain, optionRows, cancellationToken);
        }

        if (tradeRows.Count + tickerRows.Count + optionRows.Count > 0)
        {
            logger.LogInformation(
                "Flushed minute bars to Parquet. Trades: {TradeCount}, Tickers: {TickerCount}, OptionChain: {OptionCount}",
                tradeRows.Count,
                tickerRows.Count,
                optionRows.Count);
        }

        if (!includeCurrentMinute)
        {
            CleanupSeenTradeIds(cutoff.AddHours(-12));

            var reportedMinute = cutoff.AddMinutes(-1);
            if (_lastReportedMinute != reportedMinute)
            {
                _lastReportedMinute = reportedMinute;
                Console.WriteLine(
                    $"[{DateTimeOffset.UtcNow:O}] minute={reportedMinute:yyyy-MM-dd HH:mm}Z trades={tradeRows.Count} tickers={tickerRows.Count} optionChain={optionRows.Count} pendingTrades={_tradeRows.Count} pendingTickers={_tickerBars.Count} pendingOptionChain={_optionBars.Count} seenTradeIds={_seenTradeIds.Count}");
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

    private void CleanupSeenTradeIds(DateTime thresholdUtc)
    {
        foreach (var entry in _seenTradeIds)
        {
            if (entry.Value >= thresholdUtc)
            {
                continue;
            }

            _seenTradeIds.TryRemove(entry.Key, out _);
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

    private void IngestTickerAsJson<TTicker>(InstrumentDefinition instrument, TTicker payload, DateTimeOffset eventTimestamp)
    {
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(payload));
        IngestTicker(instrument, document.RootElement.Clone(), eventTimestamp);
    }

    private static decimal? ReadNullableDecimal(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return null;
        }

        if (property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        var raw = property.ValueKind == JsonValueKind.String ? property.GetString() : property.GetRawText();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var value) ? value : null;
    }

    private static DateTime? ReadNullableUnixMilliseconds(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return null;
        }

        var raw = property.ValueKind == JsonValueKind.String ? property.GetString() : property.GetRawText();
        if (string.IsNullOrWhiteSpace(raw) || !long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            return null;
        }

        return DateTimeOffset.FromUnixTimeMilliseconds(value).UtcDateTime;
    }

    private static DateTime? ReadNullableIsoDate(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return null;
        }

        var raw = property.ValueKind == JsonValueKind.String ? property.GetString() : property.GetRawText();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var value)
            ? value
            : null;
    }

    private abstract class MinuteAccumulatorBase(InstrumentDefinition instrument, DateTime minute)
    {
        protected InstrumentDefinition Instrument { get; } = instrument;
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

        public void Apply(JsonElement payload, DateTime eventTimestampUtc)
        {
            lock (_gate)
            {
                _lastPrice = ReadNullableDecimal(payload, "lastPrice") ?? _lastPrice;
                _markPrice = ReadNullableDecimal(payload, "markPrice") ?? _markPrice;
                _indexPrice = ReadNullableDecimal(payload, "indexPrice") ?? _indexPrice;
                _bidPrice = ReadNullableDecimal(payload, "bid1Price") ?? _bidPrice;
                _bidSize = ReadNullableDecimal(payload, "bid1Size") ?? _bidSize;
                _askPrice = ReadNullableDecimal(payload, "ask1Price") ?? _askPrice;
                _askSize = ReadNullableDecimal(payload, "ask1Size") ?? _askSize;
                _openInterest = ReadNullableDecimal(payload, "openInterest") ?? _openInterest;
                _openInterestValue = ReadNullableDecimal(payload, "openInterestValue") ?? _openInterestValue;
                _volume24h = ReadNullableDecimal(payload, "volume24h") ?? _volume24h;
                _turnover24h = ReadNullableDecimal(payload, "turnover24h") ?? _turnover24h;
                _fundingRate = ReadNullableDecimal(payload, "fundingRate") ?? _fundingRate;
                _basisRate = ReadNullableDecimal(payload, "basisRate") ?? _basisRate;
                _basisRateYear = ReadNullableDecimal(payload, "basisRateYear") ?? _basisRateYear;
                _deliveryUtc = ReadNullableIsoDate(payload, "deliveryTime") ?? ReadNullableUnixMilliseconds(payload, "deliveryTime") ?? _deliveryUtc;
                _nextFundingUtc = ReadNullableUnixMilliseconds(payload, "nextFundingTime") ?? _nextFundingUtc;
                _lastUpdateUtc = eventTimestampUtc;
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
                    MarketType = Instrument.MarketType,
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

        public void Apply(JsonElement payload, DateTime eventTimestampUtc)
        {
            lock (_gate)
            {
                _bidPrice = ReadNullableDecimal(payload, "bidPrice") ?? ReadNullableDecimal(payload, "bid1Price") ?? _bidPrice;
                _bidSize = ReadNullableDecimal(payload, "bidSize") ?? ReadNullableDecimal(payload, "bid1Size") ?? _bidSize;
                _bidIv = ReadNullableDecimal(payload, "bidIv") ?? ReadNullableDecimal(payload, "bid1Iv") ?? _bidIv;
                _askPrice = ReadNullableDecimal(payload, "askPrice") ?? ReadNullableDecimal(payload, "ask1Price") ?? _askPrice;
                _askSize = ReadNullableDecimal(payload, "askSize") ?? ReadNullableDecimal(payload, "ask1Size") ?? _askSize;
                _askIv = ReadNullableDecimal(payload, "askIv") ?? ReadNullableDecimal(payload, "ask1Iv") ?? _askIv;
                _lastPrice = ReadNullableDecimal(payload, "lastPrice") ?? _lastPrice;
                _markPrice = ReadNullableDecimal(payload, "markPrice") ?? _markPrice;
                _indexPrice = ReadNullableDecimal(payload, "indexPrice") ?? _indexPrice;
                _markIv = ReadNullableDecimal(payload, "markPriceIv") ?? ReadNullableDecimal(payload, "markIv") ?? _markIv;
                _underlyingPrice = ReadNullableDecimal(payload, "underlyingPrice") ?? _underlyingPrice;
                _openInterest = ReadNullableDecimal(payload, "openInterest") ?? _openInterest;
                _volume24h = ReadNullableDecimal(payload, "volume24h") ?? _volume24h;
                _turnover24h = ReadNullableDecimal(payload, "turnover24h") ?? _turnover24h;
                _totalVolume = ReadNullableDecimal(payload, "totalVolume") ?? _totalVolume;
                _totalTurnover = ReadNullableDecimal(payload, "totalTurnover") ?? _totalTurnover;
                _delta = ReadNullableDecimal(payload, "delta") ?? _delta;
                _gamma = ReadNullableDecimal(payload, "gamma") ?? _gamma;
                _vega = ReadNullableDecimal(payload, "vega") ?? _vega;
                _theta = ReadNullableDecimal(payload, "theta") ?? _theta;
                _change24h = ReadNullableDecimal(payload, "change24h") ?? _change24h;
                _lastUpdateUtc = eventTimestampUtc;
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
                    MarketType = Instrument.MarketType,
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
}
