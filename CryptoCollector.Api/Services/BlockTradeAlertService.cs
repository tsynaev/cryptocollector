using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Encodings.Web;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Api.Models;
using CryptoCollector.Api.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Api.Services;

public sealed class BlockTradeAlertService(
    DailyParquetStore store,
    ServiceStateStore stateStore,
    IOptions<BlockTradesAlertOptions> options,
    IMessageQueue messageQueue,
    ILogger<BlockTradeAlertService> logger) : BackgroundService
{
    private const string StateKey = "block-trade-alert";
    private static readonly TimeSpan PersistInterval = TimeSpan.FromSeconds(30);
    private static readonly string[] Exchanges = ["bybit", "deribit"];

    private readonly BlockTradesAlertOptions _options = options.Value;
    private readonly ConcurrentDictionary<string, BlockTradeGroup> _groups = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _stateGate = new();
    private BlockTradeAlertState _state = new();
    private volatile bool _stateDirty;
    private DateTime _lastStatePersistedUtc = DateTime.MinValue;

    public void IngestRecoveredTrade(InstrumentDefinition instrument, ExchangeTrade trade)
    {
        IngestInternal(instrument, trade, updateCursor: true);
    }

    public void IngestLiveTrade(InstrumentDefinition instrument, ExchangeTrade trade)
    {
        IngestInternal(instrument, trade, updateCursor: true);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _state = await stateStore.ReadAsync<BlockTradeAlertState>(StateKey, stoppingToken) ?? new BlockTradeAlertState();
        var exchangesToReplay = _state.Exchanges.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

        await InitializeStateAsync(exchangesToReplay, stoppingToken);
        await ReplayStoredBlockTradesAsync(exchangesToReplay, stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));

        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            FlushReadyGroups();
            if (ShouldPersistState())
            {
                await PersistStateAsync(stoppingToken);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        FlushReadyGroups(force: true);
        await PersistStateAsync(cancellationToken, force: true);
        await base.StopAsync(cancellationToken);
    }

    private async Task InitializeStateAsync(ISet<string> exchangesToReplay, CancellationToken cancellationToken)
    {
        foreach (var exchange in Exchanges)
        {
            if (_state.Exchanges.ContainsKey(exchange))
            {
                continue;
            }

            var latestTradeTimestampUtc = await store.GetLatestTimestampAsync<TradeRecord>(
                exchange,
                DataSetNames.Trades,
                cancellationToken);

            var replayFromUtc = latestTradeTimestampUtc?.AddHours(-24) ?? DateTime.UtcNow.AddHours(-24);

            _state.Exchanges[exchange] = new AlertCursor
            {
                LastCheckedTradeUtc = replayFromUtc,
                LastCheckedTradeKey = string.Empty
            };

            exchangesToReplay.Add(exchange);
            _stateDirty = true;
        }

        if (_stateDirty)
        {
            await PersistStateAsync(cancellationToken, force: true);
        }
    }

    private async Task ReplayStoredBlockTradesAsync(
        IReadOnlySet<string> exchangesWithStoredCursor,
        CancellationToken cancellationToken)
    {
        foreach (var exchange in Exchanges)
        {
            if (!exchangesWithStoredCursor.Contains(exchange) ||
                !_state.Exchanges.TryGetValue(exchange, out var cursor))
            {
                continue;
            }

            var latestTradeTimestampUtc = await store.GetLatestTimestampAsync<TradeRecord>(exchange, DataSetNames.Trades, cancellationToken);
            if (latestTradeTimestampUtc is null || latestTradeTimestampUtc.Value < cursor.LastCheckedTradeUtc)
            {
                continue;
            }

            var trades = await store.QueryAsync<TradeRecord>(
                exchange,
                DataSetNames.Trades,
                cursor.LastCheckedTradeUtc.Date,
                latestTradeTimestampUtc.Value,
                symbol: null,
                cancellationToken);

            foreach (var trade in trades
                         .OrderBy(static x => x.Date)
                         .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
                         .ThenBy(static x => x.TradeId, StringComparer.Ordinal))
            {
                IngestStoredTrade(trade);
            }
        }

        FlushReadyGroups(force: true);
        if (_stateDirty)
        {
            await PersistStateAsync(cancellationToken, force: true);
        }
    }

    private void IngestInternal(InstrumentDefinition instrument, ExchangeTrade trade, bool updateCursor)
    {
        var timestamp = trade.TradeTime.UtcDateTime;
        var tradeKey = BuildTradeKey(instrument.Exchange, instrument.Symbol, trade.TradeId, timestamp);

        if (updateCursor && !ShouldProcessTrade(instrument.Exchange, timestamp, tradeKey))
        {
            return;
        }

        if (!_options.Enabled || !trade.IsBlockTrade || string.IsNullOrWhiteSpace(trade.BlockTradeId))
        {
            return;
        }

        var price = trade.Price;
        var quantity = trade.Quantity;

        var leg = new BlockTradeLeg(
            instrument.Exchange,
            instrument.Symbol,
            instrument.MarketType,
            instrument.ExpiryUtc,
            instrument.StrikePrice,
            instrument.OptionSide,
            trade.Side,
            quantity,
            price,
            price * quantity,
            trade.BlockTradeId!,
            timestamp);

        _groups.AddOrUpdate(
            $"{instrument.Exchange}|{trade.BlockTradeId}",
            _ => new BlockTradeGroup(leg, _options.GroupWindow),
            (_, existing) =>
            {
                existing.Add(leg, _options.GroupWindow);
                return existing;
            });
    }

    private void IngestStoredTrade(TradeRecord trade)
    {
        var tradeKey = BuildTradeKey(trade.Exchange, trade.Symbol, trade.TradeId, trade.Date);
        if (!ShouldProcessTrade(trade.Exchange, trade.Date, tradeKey))
        {
            return;
        }

        if (!_options.Enabled || !trade.IsBlockTrade || string.IsNullOrWhiteSpace(trade.BlockTradeId))
        {
            return;
        }

        var leg = new BlockTradeLeg(
            trade.Exchange,
            trade.Symbol,
            trade.MarketType,
            trade.ExpiryUtc,
            trade.StrikePrice,
            trade.OptionSide,
            trade.Side,
            trade.Quantity,
            trade.Price,
            trade.Notional,
            trade.BlockTradeId!,
            trade.Date);

        _groups.AddOrUpdate(
            $"{trade.Exchange}|{trade.BlockTradeId}",
            _ => new BlockTradeGroup(leg, _options.GroupWindow),
            (_, existing) =>
            {
                existing.Add(leg, _options.GroupWindow);
                return existing;
            });
    }

    private bool ShouldProcessTrade(string exchange, DateTime timestampUtc, string tradeKey)
    {
        lock (_stateGate)
        {
            if (_state.Exchanges.TryGetValue(exchange, out var cursor))
            {
                if (timestampUtc < cursor.LastCheckedTradeUtc)
                {
                    return false;
                }

                if (timestampUtc == cursor.LastCheckedTradeUtc &&
                    string.CompareOrdinal(tradeKey, cursor.LastCheckedTradeKey) <= 0)
                {
                    return false;
                }
            }

            _state.Exchanges[exchange] = new AlertCursor
            {
                LastCheckedTradeUtc = timestampUtc,
                LastCheckedTradeKey = tradeKey
            };
            _stateDirty = true;
            return true;
        }
    }

    private bool ShouldPersistState()
    {
        if (!_stateDirty)
        {
            return false;
        }

        return DateTime.UtcNow - _lastStatePersistedUtc >= PersistInterval;
    }

    private async Task PersistStateAsync(CancellationToken cancellationToken, bool force = false)
    {
        BlockTradeAlertState snapshot;
        lock (_stateGate)
        {
            if (!_stateDirty)
            {
                return;
            }

            if (!force && DateTime.UtcNow - _lastStatePersistedUtc < PersistInterval)
            {
                return;
            }

            snapshot = new BlockTradeAlertState
            {
                Exchanges = new Dictionary<string, AlertCursor>(_state.Exchanges, StringComparer.OrdinalIgnoreCase)
            };
            _stateDirty = false;
        }

        await stateStore.WriteAsync(StateKey, snapshot, cancellationToken);
        _lastStatePersistedUtc = DateTime.UtcNow;
    }

    private void FlushReadyGroups(bool force = false)
    {
        var now = DateTime.UtcNow;

        foreach (var entry in _groups)
        {
            if (!force && entry.Value.FlushAfterUtc > now)
            {
                continue;
            }

            if (!_groups.TryRemove(entry.Key, out var group))
            {
                continue;
            }

            if (group.TotalQuantity <= _options.QuantityThreshold)
            {
                continue;
            }

            var message = FormatBlockTradeMessage(group);
            if (messageQueue.TryEnqueue(message))
            {
                logger.LogInformation(
                    "Enqueued block trade alert. Exchange={Exchange}, BlockTradeId={BlockTradeId}, Legs={LegCount}, TotalQuantity={TotalQuantity}.",
                    group.Exchange,
                    group.BlockTradeId,
                    group.Legs.Count,
                    group.TotalQuantity);
            }
        }
    }

    private static string FormatBlockTradeMessage(BlockTradeGroup group)
    {
        var encoder = HtmlEncoder.Default;
        var orderedLegs = group.Legs
            .OrderBy(static x => x.TimestampUtc)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var timestamp = orderedLegs[0].TimestampUtc;

        return string.Join('\n',
        [
            $"<b>BLOCK TRADE {encoder.Encode(group.Exchange.ToUpperInvariant())}</b>",
            $"{timestamp:dd MMMM yyyy HH:mm:ss} UTC",
            .. orderedLegs.Select(FormatLeg),
            $"<code>{encoder.Encode(group.BlockTradeId)}</code>"
        ]);
    }

    private static string FormatLeg(BlockTradeLeg leg)
    {
        var side = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(leg.Side.ToLowerInvariant());
        var optionSide = string.IsNullOrWhiteSpace(leg.OptionSide)
            ? string.Empty
            : leg.OptionSide!.ToUpperInvariant();
        var quantity = leg.Quantity.ToString("0.####", CultureInfo.InvariantCulture);
        var price = leg.Price.ToString("0.0000", CultureInfo.InvariantCulture);
        var premium = leg.Notional.ToString("0.0000", CultureInfo.InvariantCulture);

        if (leg.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
        {
            var strike = leg.StrikePrice?.ToString(CultureInfo.InvariantCulture) ?? "-";
            var expiry = leg.ExpiryUtc?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? "-";
            return $"{side} {quantity} {optionSide} {strike} Exp. {expiry} @ {premium}";
        }

        if (leg.Exchange.Equals("deribit", StringComparison.OrdinalIgnoreCase) && leg.Price > 0)
        {
            var displayQuantity = (leg.Quantity / leg.Price).ToString("0.####", CultureInfo.InvariantCulture);
            return $"{side} {displayQuantity} {leg.Symbol} @ {price}";
        }

        return $"{side} {quantity} {leg.Symbol} @ {price}";
    }

    private static string BuildTradeKey(string exchange, string symbol, string tradeId, DateTime timestampUtc) =>
        $"{exchange}|{symbol}|{tradeId}|{timestampUtc:O}";

    private sealed class BlockTradeGroup
    {
        private readonly Lock _gate = new();
        private readonly List<BlockTradeLeg> _legs = [];

        public BlockTradeGroup(BlockTradeLeg firstLeg, TimeSpan groupWindow)
        {
            Exchange = firstLeg.Exchange;
            BlockTradeId = firstLeg.BlockTradeId;
            _legs.Add(firstLeg);
            TotalQuantity = firstLeg.Quantity;
            FlushAfterUtc = firstLeg.TimestampUtc.Add(groupWindow);
        }

        public string Exchange { get; }
        public string BlockTradeId { get; }
        public DateTime FlushAfterUtc { get; private set; }
        public decimal TotalQuantity { get; private set; }

        public IReadOnlyList<BlockTradeLeg> Legs
        {
            get
            {
                lock (_gate)
                {
                    return _legs.ToArray();
                }
            }
        }

        public void Add(BlockTradeLeg leg, TimeSpan groupWindow)
        {
            lock (_gate)
            {
                _legs.Add(leg);
                TotalQuantity += leg.Quantity;
                var candidateFlushTime = leg.TimestampUtc.Add(groupWindow);
                if (candidateFlushTime > FlushAfterUtc)
                {
                    FlushAfterUtc = candidateFlushTime;
                }
            }
        }
    }

    private sealed record BlockTradeLeg(
        string Exchange,
        string Symbol,
        string MarketType,
        DateTime? ExpiryUtc,
        decimal? StrikePrice,
        string? OptionSide,
        string Side,
        decimal Quantity,
        decimal Price,
        decimal Notional,
        string BlockTradeId,
        DateTime TimestampUtc);
}
