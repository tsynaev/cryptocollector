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
    PositionPnlChartRenderer chartRenderer,
    BlackScholesPricer blackScholesPricer,
    ILogger<BlockTradeAlertService> logger) : BackgroundService
{
    private const string StateKey = "block-trade-alert";
    private const int TelegramCaptionLimit = 1024;
    private static readonly TimeSpan PersistInterval = TimeSpan.FromSeconds(30);
    private static readonly string[] Exchanges = ["bybit", "deribit"];

    private readonly BlockTradesAlertOptions _options = options.Value;
    private readonly ConcurrentDictionary<string, BlockTradeGroup> _groups = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<PendingTrade> _pendingTrades = new();
    private readonly Lock _stateGate = new();
    private BlockTradeAlertState _state = new();
    private volatile bool _stateDirty;
    private volatile bool _startupReplayCompleted;
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
        DrainPendingTrades();
        _startupReplayCompleted = true;

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));

        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            await FlushReadyGroupsAsync(stoppingToken);
            if (ShouldPersistState())
            {
                await PersistStateAsync(stoppingToken);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await FlushReadyGroupsAsync(cancellationToken, force: true);
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

            var replayFromUtc = DateTime.UtcNow.AddHours(-24);

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

        await FlushReadyGroupsAsync(cancellationToken, force: true);
        if (_stateDirty)
        {
            await PersistStateAsync(cancellationToken, force: true);
        }
    }

    private void IngestInternal(InstrumentDefinition instrument, ExchangeTrade trade, bool updateCursor)
    {
        if (!_startupReplayCompleted)
        {
            _pendingTrades.Enqueue(new PendingTrade(instrument, trade, updateCursor));
            return;
        }

        IngestCore(instrument, trade, updateCursor);
    }

    private void IngestCore(InstrumentDefinition instrument, ExchangeTrade trade, bool updateCursor)
    {
        var timestamp = trade.TradeTime.UtcDateTime;
        var tradeKey = BuildTradeKey(instrument.Exchange, instrument.Symbol, trade.TradeId, timestamp);

        if (updateCursor && !ShouldProcessTrade(instrument.Exchange, timestamp, tradeKey))
        {
            return;
        }

        if (!_options.Enabled || !TryResolveAlertGroupKey(trade, out var groupKey, out var groupId, out var groupType))
        {
            return;
        }

        var price = trade.Price;
        var quantity = trade.Quantity;

        var leg = new BlockTradeLeg(
            instrument.Exchange,
            instrument.Symbol,
            instrument.MarketType,
            instrument.BaseAsset,
            instrument.QuoteAsset,
            instrument.SettleAsset,
            instrument.ExpiryUtc,
            instrument.StrikePrice,
            instrument.OptionSide,
            trade.Side,
            quantity,
            trade.Contracts,
            trade.Amount,
            price,
            trade.MarkPrice,
            trade.IndexPrice,
            trade.Iv,
            trade.MarkIv,
            price * quantity,
            groupId,
            groupType,
            timestamp);

        _groups.AddOrUpdate(
            $"{instrument.Exchange}|{groupKey}",
            _ => new BlockTradeGroup(leg, _options.GroupWindow),
            (_, existing) =>
            {
                existing.Add(leg, _options.GroupWindow);
                return existing;
            });
    }

    private void DrainPendingTrades()
    {
        if (_pendingTrades.IsEmpty)
        {
            return;
        }

        var bufferedTrades = new List<PendingTrade>();
        while (_pendingTrades.TryDequeue(out var pendingTrade))
        {
            bufferedTrades.Add(pendingTrade);
        }

        foreach (var pendingTrade in bufferedTrades
                     .OrderBy(static x => x.Trade.TradeTime.UtcDateTime)
                     .ThenBy(static x => x.Instrument.Symbol, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(static x => x.Trade.TradeId, StringComparer.Ordinal))
        {
            IngestCore(pendingTrade.Instrument, pendingTrade.Trade, pendingTrade.UpdateCursor);
        }
    }

    private void IngestStoredTrade(TradeRecord trade)
    {
        var tradeKey = BuildTradeKey(trade.Exchange, trade.Symbol, trade.TradeId, trade.Date);
        if (!ShouldProcessTrade(trade.Exchange, trade.Date, tradeKey))
        {
            return;
        }

        if (!_options.Enabled || !TryResolveAlertGroupKey(trade, out var groupKey, out var groupId, out var groupType))
        {
            return;
        }

        var leg = new BlockTradeLeg(
            trade.Exchange,
            trade.Symbol,
            trade.MarketType,
            trade.BaseAsset,
            trade.QuoteAsset,
            trade.SettleAsset,
            trade.ExpiryUtc,
            trade.StrikePrice,
            trade.OptionSide,
            trade.Side,
            trade.Quantity,
            trade.Contracts,
            trade.Amount,
            trade.Price,
            trade.MarkPrice,
            trade.IndexPrice,
            trade.Iv,
            trade.MarkIv,
            trade.Notional,
            groupId,
            groupType,
            trade.Date);

        _groups.AddOrUpdate(
            $"{trade.Exchange}|{groupKey}",
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

    private async Task FlushReadyGroupsAsync(CancellationToken cancellationToken, bool force = false)
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

            var totalUsdNotional = await ResolveGroupUsdNotionalAsync(group, cancellationToken);
            if (totalUsdNotional < _options.MinGroupUsd)
            {
                continue;
            }

            var message = await FormatBlockTradeMessageAsync(group, cancellationToken);
            var caption = TrimCaption(message);
            var enqueued = await TryEnqueueAlertAsync(group, caption, cancellationToken);
            if (enqueued)
            {
                logger.LogInformation(
                    "Enqueued block trade alert. Exchange={Exchange}, GroupType={GroupType}, GroupId={GroupId}, Legs={LegCount}, TotalQuantity={TotalQuantity}, TotalUsdNotional={TotalUsdNotional}.",
                    group.Exchange,
                    group.GroupType,
                    group.GroupId,
                    group.Legs.Count,
                    group.TotalQuantity,
                    totalUsdNotional);
            }
        }
    }

    private async Task<bool> TryEnqueueAlertAsync(
        BlockTradeGroup group,
        string caption,
        CancellationToken cancellationToken)
    {
        try
        {
            var chart = await BuildPositionPnlChartAsync(group, cancellationToken);
            if (chart is null)
            {
                return messageQueue.TryEnqueue(new OutboundMessage(caption));
            }

            var photoBytes = chartRenderer.Render(chart);
            return messageQueue.TryEnqueue(new OutboundMessage(caption, photoBytes, "block-trade-pnl.png"));
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to render block trade chart. Exchange={Exchange}, GroupId={GroupId}, GroupType={GroupType}. Falling back to text alert.",
                group.Exchange,
                group.GroupId,
                group.GroupType);
            return messageQueue.TryEnqueue(new OutboundMessage(caption));
        }
    }

    private async Task<string> FormatBlockTradeMessageAsync(BlockTradeGroup group, CancellationToken cancellationToken)
    {
        var encoder = HtmlEncoder.Default;
        var orderedLegs = group.Legs
            .OrderBy(static x => x.TimestampUtc)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var timestamp = orderedLegs[0].TimestampUtc;
        var legLines = await FormatLegLinesAsync(orderedLegs, cancellationToken);
        var baseAsset = orderedLegs
            .Select(static x => x.BaseAsset)
            .FirstOrDefault(static x => !string.IsNullOrWhiteSpace(x));
        var headerSuffix = string.IsNullOrWhiteSpace(baseAsset)
            ? string.Empty
            : $" ({encoder.Encode(baseAsset.ToUpperInvariant())})";

        return string.Join('\n',
        [
            $"<b>BLOCK TRADE {encoder.Encode(group.Exchange.ToUpperInvariant())}{headerSuffix}</b>",
            $"{timestamp:dd MMMM yyyy HH:mm:ss} UTC",
            string.Empty,
            .. legLines,
            $"<code>{encoder.Encode(group.GroupId)}</code>"
        ]);
    }

    private async Task<IReadOnlyList<string>> FormatLegLinesAsync(
        IReadOnlyList<BlockTradeLeg> orderedLegs,
        CancellationToken cancellationToken)
    {
        var lines = new List<string>(orderedLegs.Count * 2);
        var optionGroups = orderedLegs
            .Where(static x => x.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
            .GroupBy(static x => x.ExpiryUtc)
            .OrderBy(static x => x.Key ?? DateTime.MaxValue)
            .ToArray();
        var nonOptionLegs = orderedLegs
            .Where(static x => !x.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        foreach (var optionGroup in optionGroups)
        {
            if (lines.Count > 0)
            {
                lines.Add(string.Empty);
            }

            var expiry = optionGroup.Key?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? "-";
            lines.Add($"Exp. {expiry}");

            foreach (var leg in optionGroup
                         .OrderBy(static x => x.TimestampUtc)
                         .ThenBy(static x => x.StrikePrice ?? decimal.MaxValue)
                         .ThenBy(static x => x.OptionSide, StringComparer.OrdinalIgnoreCase))
            {
                lines.Add(await FormatLegAsync(leg, cancellationToken));
            }
        }

        if (nonOptionLegs.Length > 0 && lines.Count > 0)
        {
            lines.Add(string.Empty);
        }

        foreach (var leg in nonOptionLegs)
        {
            lines.Add(await FormatLegAsync(leg, cancellationToken));
        }

        return lines;
    }

    private async Task<string> FormatLegAsync(BlockTradeLeg leg, CancellationToken cancellationToken)
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
            if (await TryConvertOptionAmountsAsync(leg, cancellationToken) is { } converted)
            {
                return $"{side} {quantity} {optionSide} {strike} @ {converted.UnitPriceText} ({converted.TotalPremiumValueText})";
            }

            var settleAsset = leg.SettleAsset;
            return $"{side} {quantity} {optionSide} {strike} @ {price} {settleAsset} ({premium} {settleAsset})";
        }

        var displayQuantityValue = ResolveDisplayQuantity(leg);
        if (displayQuantityValue != leg.Quantity)
        {
            var displayQuantity = displayQuantityValue.ToString("0.####", CultureInfo.InvariantCulture);
            return $"{side} {displayQuantity} {leg.Symbol} @ {price}";
        }

        return $"{side} {quantity} {leg.Symbol} @ {price}";
    }

    private async Task<PositionPnlChart?> BuildPositionPnlChartAsync(
        BlockTradeGroup group,
        CancellationToken cancellationToken)
    {
        var orderedLegs = group.Legs
            .OrderBy(static x => x.TimestampUtc)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (orderedLegs.Length == 0)
        {
            return null;
        }

        var evaluatedLegs = await EvaluateLegsAsync(orderedLegs, cancellationToken);
        var firstLeg = evaluatedLegs[0].Leg;
        var currentSpot = evaluatedLegs
            .Select(static x => x.UnderlyingPrice)
            .FirstOrDefault(static x => x is > 0);
        var strikeValues = orderedLegs
            .Where(static x => x.StrikePrice is > 0)
            .Select(static x => x.StrikePrice!.Value)
            .ToArray();
        var referenceSpot = currentSpot
            ?? (strikeValues.Length > 0 ? strikeValues.Average() : 0m);
        if (referenceSpot <= 0)
        {
            return null;
        }

        var minReference = strikeValues.Length > 0
            ? Math.Min(strikeValues.Min(), referenceSpot)
            : referenceSpot;
        var maxReference = strikeValues.Length > 0
            ? Math.Max(strikeValues.Max(), referenceSpot)
            : referenceSpot;
        var xMin = Math.Max(1m, minReference * 0.5m);
        var xMax = Math.Max(xMin + 1m, maxReference * 1.5m);
        const int pointCount = 241;
        var prices = new decimal[pointCount];
        var expiryPnl = new decimal[pointCount];
        var currentPnl = new decimal[pointCount];
        var displayCurrency = ResolveDisplayCurrency(firstLeg);

        for (var index = 0; index < pointCount; index++)
        {
            var price = xMin + (xMax - xMin) * index / (pointCount - 1);
            prices[index] = price;
            expiryPnl[index] = ResolveGroupPnlAtExpiry(evaluatedLegs, price);
            currentPnl[index] = ResolveGroupPnlAtCurrentTime(evaluatedLegs, price);
        }

        return new PositionPnlChart(
            displayCurrency,
            currentSpot ?? referenceSpot,
            prices,
            expiryPnl,
            currentPnl,
            DateTime.UtcNow);
    }

    private async Task<IReadOnlyList<EvaluatedBlockTradeLeg>> EvaluateLegsAsync(
        IReadOnlyList<BlockTradeLeg> legs,
        CancellationToken cancellationToken)
    {
        var evaluated = new EvaluatedBlockTradeLeg[legs.Count];

        for (var index = 0; index < legs.Count; index++)
        {
            var leg = legs[index];
            var underlyingPrice = await ResolveUnderlyingPriceAsync(leg, cancellationToken);
            var premiumUsd = await ResolveLegUsdNotionalAsync(leg, underlyingPrice);
            var volatility = ResolveVolatility(leg);
            var timeToExpiryYears = ResolveTimeToExpiryYears(leg);

            evaluated[index] = new EvaluatedBlockTradeLeg(
                leg,
                ResolveDisplayQuantity(leg),
                underlyingPrice,
                premiumUsd,
                volatility,
                timeToExpiryYears);
        }

        return evaluated;
    }

    private decimal ResolveGroupPnlAtExpiry(
        IReadOnlyList<EvaluatedBlockTradeLeg> legs,
        decimal underlyingPrice)
    {
        decimal total = 0m;

        foreach (var leg in legs)
        {
            total += ResolveLegPnlAtExpiry(leg, underlyingPrice);
        }

        return total;
    }

    private decimal ResolveGroupPnlAtCurrentTime(
        IReadOnlyList<EvaluatedBlockTradeLeg> legs,
        decimal underlyingPrice)
    {
        decimal total = 0m;

        foreach (var leg in legs)
        {
            total += ResolveLegPnlAtCurrentTime(leg, underlyingPrice);
        }

        return total;
    }

    private decimal ResolveLegPnlAtExpiry(EvaluatedBlockTradeLeg leg, decimal underlyingPrice)
    {
        var sideSign = leg.Leg.Side.Equals("buy", StringComparison.OrdinalIgnoreCase) ? 1m : -1m;

        if (leg.Leg.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
        {
            if (leg.Leg.StrikePrice is null || string.IsNullOrWhiteSpace(leg.Leg.OptionSide))
            {
                return 0m;
            }

            var intrinsicPerContract = blackScholesPricer.IntrinsicValue(
                leg.Leg.OptionSide.Equals("call", StringComparison.OrdinalIgnoreCase),
                underlyingPrice,
                leg.Leg.StrikePrice.Value);

            return sideSign * ((intrinsicPerContract * leg.DisplayQuantity) - leg.PremiumUsd);
        }

        return sideSign * leg.DisplayQuantity * (underlyingPrice - leg.Leg.Price);
    }

    private decimal ResolveLegPnlAtCurrentTime(EvaluatedBlockTradeLeg leg, decimal underlyingPrice)
    {
        var sideSign = leg.Leg.Side.Equals("buy", StringComparison.OrdinalIgnoreCase) ? 1m : -1m;

        if (leg.Leg.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
        {
            if (leg.Leg.StrikePrice is null || string.IsNullOrWhiteSpace(leg.Leg.OptionSide))
            {
                return 0m;
            }

            var optionValue = blackScholesPricer.Price(
                leg.Leg.OptionSide.Equals("call", StringComparison.OrdinalIgnoreCase),
                underlyingPrice,
                leg.Leg.StrikePrice.Value,
                leg.TimeToExpiryYears,
                leg.Volatility);

            return sideSign * ((optionValue * leg.DisplayQuantity) - leg.PremiumUsd);
        }

        return sideSign * leg.DisplayQuantity * (underlyingPrice - leg.Leg.Price);
    }

    private async Task<ConvertedOptionAmounts?> TryConvertOptionAmountsAsync(BlockTradeLeg leg, CancellationToken cancellationToken)
    {
        if (!leg.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase) ||
            !IsPriceQuotedInBaseAsset(leg))
        {
            return null;
        }

        var underlyingPrice = await ResolveUnderlyingPriceAsync(leg, cancellationToken);
        if (underlyingPrice is null || underlyingPrice <= 0)
        {
            return null;
        }

        var displayCurrency = ResolveStableDisplayCurrency(leg);
        var unitPrice = leg.Price * underlyingPrice.Value;
        var totalPremium = leg.Notional * underlyingPrice.Value;

        return new ConvertedOptionAmounts(
            $"{unitPrice.ToString("0.00", CultureInfo.InvariantCulture)} {displayCurrency}",
            $"{totalPremium.ToString("0.00", CultureInfo.InvariantCulture)}",
            $"{totalPremium.ToString("0.00", CultureInfo.InvariantCulture)} {displayCurrency}");
    }

    private async Task<decimal> ResolveGroupUsdNotionalAsync(BlockTradeGroup group, CancellationToken cancellationToken)
    {
        decimal total = 0m;

        foreach (var leg in group.Legs)
        {
            total += await ResolveLegUsdNotionalAsync(leg, cancellationToken);
        }

        return total;
    }

    private async Task<decimal> ResolveLegUsdNotionalAsync(BlockTradeLeg leg, CancellationToken cancellationToken)
    {
        return await ResolveLegUsdNotionalAsync(
            leg,
            await ResolveUnderlyingPriceAsync(leg, cancellationToken));
    }

    private Task<decimal> ResolveLegUsdNotionalAsync(BlockTradeLeg leg, decimal? underlyingPrice)
    {
        if (leg.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase))
        {
            if (IsUsdLike(leg.SettleAsset) || IsUsdLike(leg.QuoteAsset))
            {
                return Task.FromResult(Math.Abs(leg.Notional));
            }

            if (IsPriceQuotedInBaseAsset(leg))
            {
                return Task.FromResult(underlyingPrice is > 0 ? Math.Abs(leg.Notional * underlyingPrice.Value) : 0m);
            }

            return Task.FromResult(0m);
        }

        if (leg.Amount is not null && IsUsdLike(leg.QuoteAsset))
        {
            return Task.FromResult(Math.Abs(leg.Amount.Value));
        }

        if (IsUsdLike(leg.QuoteAsset) || IsUsdLike(leg.SettleAsset))
        {
            return Task.FromResult(Math.Abs(leg.Price * ResolveDisplayQuantity(leg)));
        }

        return Task.FromResult(underlyingPrice is > 0
            ? Math.Abs(ResolveDisplayQuantity(leg) * underlyingPrice.Value)
            : 0m);
    }

    private async Task<decimal?> ResolveUnderlyingPriceAsync(BlockTradeLeg leg, CancellationToken cancellationToken)
    {
        if (leg.IndexPrice is > 0)
        {
            return leg.IndexPrice.Value;
        }

        if (leg.MarkPrice is > 0)
        {
            return leg.MarkPrice.Value;
        }

        return await TryResolveUnderlyingPriceAsync(leg, requireTimestampMatch: true, cancellationToken) ??
               await TryResolveUnderlyingPriceAsync(leg, requireTimestampMatch: false, cancellationToken);
    }

    private async Task<decimal?> TryResolveUnderlyingPriceAsync(
        BlockTradeLeg leg,
        bool requireTimestampMatch,
        CancellationToken cancellationToken)
    {
        var directOptionSnapshots = await store.QueryLatestAsync<OptionChainMinuteBar>(
            leg.Exchange,
            DataSetNames.OptionChain,
            leg.Symbol,
            row => MatchesUnderlyingSnapshot(row, leg, requireTimestampMatch),
            cancellationToken);
        var directOptionSnapshot = directOptionSnapshots
            .OrderByDescending(static x => x.Date)
            .FirstOrDefault();

        var directUnderlyingPrice = directOptionSnapshot?.UnderlyingPrice ?? directOptionSnapshot?.IndexPrice;
        if (directUnderlyingPrice is not null && directUnderlyingPrice > 0)
        {
            return directUnderlyingPrice;
        }

        var baseAssetOptionSnapshots = await store.QueryLatestAsync<OptionChainMinuteBar>(
            leg.Exchange,
            DataSetNames.OptionChain,
            symbol: null,
            row => row.BaseAsset.Equals(leg.BaseAsset, StringComparison.OrdinalIgnoreCase) &&
                   MatchesUnderlyingSnapshot(row, leg, requireTimestampMatch),
            cancellationToken);
        var baseAssetOptionSnapshot = baseAssetOptionSnapshots
            .OrderByDescending(static x => x.Date)
            .FirstOrDefault();

        var optionUnderlyingPrice = baseAssetOptionSnapshot?.UnderlyingPrice ?? baseAssetOptionSnapshot?.IndexPrice;
        if (optionUnderlyingPrice is not null && optionUnderlyingPrice > 0)
        {
            return optionUnderlyingPrice;
        }

        var tickerSnapshots = await store.QueryLatestAsync<TickerMinuteBar>(
            leg.Exchange,
            DataSetNames.Tickers,
            symbol: null,
            row => row.BaseAsset.Equals(leg.BaseAsset, StringComparison.OrdinalIgnoreCase) &&
                   !row.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase) &&
                   MatchesUnderlyingSnapshot(row, leg, requireTimestampMatch),
            cancellationToken);
        var tickerSnapshot = tickerSnapshots
            .OrderByDescending(static x => x.Date)
            .FirstOrDefault();

        return tickerSnapshot?.IndexPrice ?? tickerSnapshot?.MarkPrice ?? tickerSnapshot?.LastPrice;
    }

    private static string ResolveStableDisplayCurrency(BlockTradeLeg leg)
    {
        if (leg.QuoteAsset.Equals("USDT", StringComparison.OrdinalIgnoreCase) ||
            leg.QuoteAsset.Equals("USDC", StringComparison.OrdinalIgnoreCase) ||
            leg.QuoteAsset.Equals("USD", StringComparison.OrdinalIgnoreCase))
        {
            return leg.QuoteAsset.ToUpperInvariant();
        }

        if (leg.Symbol.Contains("USDC", StringComparison.OrdinalIgnoreCase))
        {
            return "USDC";
        }

        if (leg.Symbol.Contains("USDT", StringComparison.OrdinalIgnoreCase))
        {
            return "USDT";
        }

        return "USDT";
    }

    private static string ResolveDisplayCurrency(BlockTradeLeg leg)
    {
        if (IsPriceQuotedInBaseAsset(leg))
        {
            return ResolveStableDisplayCurrency(leg);
        }

        if (IsUsdLike(leg.QuoteAsset))
        {
            return leg.QuoteAsset.ToUpperInvariant();
        }

        if (IsUsdLike(leg.SettleAsset))
        {
            return leg.SettleAsset.ToUpperInvariant();
        }

        return ResolveStableDisplayCurrency(leg);
    }

    private static bool IsPriceQuotedInBaseAsset(BlockTradeLeg leg) =>
        leg.SettleAsset.Equals(leg.BaseAsset, StringComparison.OrdinalIgnoreCase);

    private static bool IsUsdLike(string? asset) =>
        asset is not null &&
        (asset.Equals("USD", StringComparison.OrdinalIgnoreCase) ||
         asset.Equals("USDT", StringComparison.OrdinalIgnoreCase) ||
         asset.Equals("USDC", StringComparison.OrdinalIgnoreCase));

    private static decimal ResolveDisplayQuantity(BlockTradeLeg leg)
    {
        if (!leg.MarketType.Equals("option", StringComparison.OrdinalIgnoreCase) &&
            IsUsdLike(leg.QuoteAsset) &&
            IsPriceQuotedInBaseAsset(leg))
        {
            var referencePrice = leg.IndexPrice ?? leg.MarkPrice;
            if (referencePrice is > 0)
            {
                return leg.Quantity / referencePrice.Value;
            }
        }

        return leg.Quantity;
    }

    private decimal ResolveVolatility(BlockTradeLeg leg) =>
        blackScholesPricer.NormalizeVolatility(leg.MarkIv ?? leg.Iv);

    private static decimal ResolveTimeToExpiryYears(BlockTradeLeg leg)
    {
        if (leg.ExpiryUtc is null)
        {
            return 0m;
        }

        var timeToExpiry = leg.ExpiryUtc.Value - DateTime.UtcNow;
        if (timeToExpiry <= TimeSpan.Zero)
        {
            return 0m;
        }

        return Math.Max((decimal)timeToExpiry.TotalDays / 365m, 0.0001m);
    }

    private static string TrimCaption(string message)
    {
        if (message.Length <= TelegramCaptionLimit)
        {
            return message;
        }

        var lines = message.Split('\n');
        var buffer = new List<string>(lines.Length);
        var currentLength = 0;

        foreach (var line in lines)
        {
            var additionalLength = line.Length + (buffer.Count > 0 ? 1 : 0);
            if (currentLength + additionalLength > TelegramCaptionLimit - 3)
            {
                break;
            }

            buffer.Add(line);
            currentLength += additionalLength;
        }

        if (buffer.Count == 0)
        {
            return message[..Math.Min(message.Length, TelegramCaptionLimit - 3)] + "...";
        }

        return string.Join('\n', buffer) + "...";
    }

    private static bool MatchesUnderlyingSnapshot(OptionChainMinuteBar row, BlockTradeLeg leg, bool requireTimestampMatch) =>
        (!requireTimestampMatch || row.Date <= leg.TimestampUtc) &&
        ((row.UnderlyingPrice ?? row.IndexPrice) ?? 0m) > 0;

    private static bool MatchesUnderlyingSnapshot(TickerMinuteBar row, BlockTradeLeg leg, bool requireTimestampMatch) =>
        (!requireTimestampMatch || row.Date <= leg.TimestampUtc) &&
        ((row.IndexPrice ?? row.MarkPrice ?? row.LastPrice) ?? 0m) > 0;

    private static string BuildTradeKey(string exchange, string symbol, string tradeId, DateTime timestampUtc) =>
        $"{exchange}|{symbol}|{tradeId}|{timestampUtc:O}";

    private static bool TryResolveAlertGroupKey(
        ExchangeTrade trade,
        out string groupKey,
        out string groupId,
        out string groupType)
    {
        if (TryResolveAlertGroupIdentity(
                "block_trade_id",
                trade.BlockTradeId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        if (TryResolveAlertGroupIdentity(
                "block_rfq_id",
                trade.BlockRfqId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        if (TryResolveAlertGroupIdentity(
                "combo_trade_id",
                trade.ComboTradeId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        if (TryResolveAlertGroupIdentity(
                "combo_id",
                trade.ComboId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        groupKey = string.Empty;
        groupId = string.Empty;
        groupType = string.Empty;
        return false;
    }

    private static bool TryResolveAlertGroupKey(
        TradeRecord trade,
        out string groupKey,
        out string groupId,
        out string groupType)
    {
        if (TryResolveAlertGroupIdentity(
                "block_trade_id",
                trade.BlockTradeId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        if (TryResolveAlertGroupIdentity(
                "block_rfq_id",
                trade.BlockRfqId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        if (TryResolveAlertGroupIdentity(
                "combo_trade_id",
                trade.ComboTradeId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        if (TryResolveAlertGroupIdentity(
                "combo_id",
                trade.ComboId,
                out groupId,
                out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        groupKey = string.Empty;
        groupId = string.Empty;
        groupType = string.Empty;
        return false;
    }

    private static bool TryResolveAlertGroupIdentity(
        string type,
        string? value,
        out string groupId,
        out string groupType)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            groupId = value;
            groupType = type;
            return true;
        }

        groupId = string.Empty;
        groupType = string.Empty;
        return false;
    }

    private sealed class BlockTradeGroup
    {
        private readonly Lock _gate = new();
        private readonly List<BlockTradeLeg> _legs = [];

        public BlockTradeGroup(BlockTradeLeg firstLeg, TimeSpan groupWindow)
        {
            Exchange = firstLeg.Exchange;
            GroupId = firstLeg.GroupId;
            GroupType = firstLeg.GroupType;
            _legs.Add(firstLeg);
            TotalQuantity = firstLeg.Quantity;
            FlushAfterUtc = firstLeg.TimestampUtc.Add(groupWindow);
        }

        public string Exchange { get; }
        public string GroupId { get; }
        public string GroupType { get; }
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
        string BaseAsset,
        string QuoteAsset,
        string SettleAsset,
        DateTime? ExpiryUtc,
        decimal? StrikePrice,
        string? OptionSide,
        string Side,
        decimal Quantity,
        decimal? Contracts,
        decimal? Amount,
        decimal Price,
        decimal? MarkPrice,
        decimal? IndexPrice,
        decimal? Iv,
        decimal? MarkIv,
        decimal Notional,
        string GroupId,
        string GroupType,
        DateTime TimestampUtc);

    private sealed record EvaluatedBlockTradeLeg(
        BlockTradeLeg Leg,
        decimal DisplayQuantity,
        decimal? UnderlyingPrice,
        decimal PremiumUsd,
        decimal Volatility,
        decimal TimeToExpiryYears);

    private sealed record ConvertedOptionAmounts(string UnitPriceText, string TotalPremiumValueText, string TotalPremiumText);
    private sealed record PendingTrade(InstrumentDefinition Instrument, ExchangeTrade Trade, bool UpdateCursor);
}
