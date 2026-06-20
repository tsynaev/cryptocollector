using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Api.Models;

namespace CryptoCollector.Api.Services;

public static class BlockTradeHistoryBuilder
{
    public static IReadOnlyList<BlockTradeHistoryGroup> Build(
        IEnumerable<TradeRecord> trades,
        decimal minGroupUsd)
    {
        var groups = new Dictionary<string, List<TradeRecord>>(StringComparer.OrdinalIgnoreCase);
        var metadata = new Dictionary<string, (string Exchange, string GroupId, string GroupType)>(StringComparer.OrdinalIgnoreCase);

        foreach (var trade in trades)
        {
            if (!TryResolveGroup(trade, out var groupKey, out var groupId, out var groupType))
            {
                continue;
            }

            var bucketKey = $"{trade.Exchange}|{groupKey}";
            if (!groups.TryGetValue(bucketKey, out var bucket))
            {
                bucket = [];
                groups[bucketKey] = bucket;
                metadata[bucketKey] = (trade.Exchange, groupId, groupType);
            }

            bucket.Add(trade);
        }

        return groups
            .Select(entry =>
            {
                var groupMetadata = metadata[entry.Key];
                var orderedLegs = entry.Value
                    .OrderByDescending(static x => x.Date)
                    .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
                    .Select(MapLeg)
                    .ToArray();

                return new BlockTradeHistoryGroup
                {
                    Exchange = groupMetadata.Exchange,
                    GroupId = groupMetadata.GroupId,
                    GroupType = groupMetadata.GroupType,
                    TimestampUtc = orderedLegs.Max(static x => x.Date),
                    BaseAsset = orderedLegs.Select(static x => x.BaseAsset).FirstOrDefault(static x => !string.IsNullOrWhiteSpace(x)),
                    TotalQuantity = orderedLegs.Sum(static x => x.Quantity),
                    TotalUsdNotional = orderedLegs.Sum(static x => x.UsdNotional),
                    LegCount = orderedLegs.Length,
                    Legs = orderedLegs
                };
            })
            .OrderByDescending(static x => x.TimestampUtc)
            .ThenBy(static x => x.Exchange, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static BlockTradeHistoryLeg MapLeg(TradeRecord trade) =>
        new()
        {
            Symbol = trade.Symbol,
            InstrumentType = trade.InstrumentType,
            BaseAsset = trade.BaseAsset,
            QuoteAsset = trade.QuoteAsset,
            SettleAsset = trade.SettleAsset,
            Date = trade.Date,
            ExpiryUtc = trade.ExpiryUtc,
            StrikePrice = trade.StrikePrice,
            OptionSide = trade.OptionSide,
            TradeId = trade.TradeId,
            Side = trade.Side,
            Quantity = trade.Quantity,
            Contracts = trade.Contracts,
            Amount = trade.Amount,
            Price = trade.Price,
            MarkPrice = trade.MarkPrice,
            IndexPrice = trade.IndexPrice,
            Iv = trade.Iv,
            MarkIv = trade.MarkIv,
            UsdNotional = Math.Abs(trade.Notional),
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

    private static bool TryResolveGroup(
        TradeRecord trade,
        out string groupKey,
        out string groupId,
        out string groupType)
    {
        if (TryResolveStructuredGroupKey(
                trade.BlockTradeId,
                trade.BlockRfqId,
                trade.ComboTradeId,
                trade.ComboId,
                out groupKey,
                out groupId,
                out groupType))
        {
            return true;
        }

        groupKey = string.Empty;
        groupId = string.Empty;
        groupType = string.Empty;
        return false;
    }

    private static bool TryResolveStructuredGroupKey(
        string? blockTradeId,
        string? blockRfqId,
        string? comboTradeId,
        string? comboId,
        out string groupKey,
        out string groupId,
        out string groupType)
    {
        if (TryResolveGroupIdentity("block_trade_id", blockTradeId, out groupId, out groupType) ||
            TryResolveGroupIdentity("block_rfq_id", blockRfqId, out groupId, out groupType) ||
            TryResolveGroupIdentity("combo_trade_id", comboTradeId, out groupId, out groupType) ||
            TryResolveGroupIdentity("combo_id", comboId, out groupId, out groupType))
        {
            groupKey = $"{groupType}|{groupId}";
            return true;
        }

        groupKey = string.Empty;
        groupId = string.Empty;
        groupType = string.Empty;
        return false;
    }

    private static bool TryResolveGroupIdentity(
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
}
