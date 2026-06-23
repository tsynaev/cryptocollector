using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.Api.Services;

internal static class BlockTradeCandidateResolver
{
    public static bool TryResolveCandidate(
        InstrumentDefinition instrument,
        ExchangeTrade trade,
        decimal minGroupUsd,
        out string groupId,
        out string groupType,
        out decimal usdNotional)
    {
        usdNotional = EstimateStandaloneTradeUsdNotional(
            instrument.InstrumentType,
            instrument.BaseAsset,
            instrument.QuoteAsset,
            instrument.SettleAsset,
            trade.Quantity,
            trade.Amount,
            trade.Price,
            trade.IndexPrice,
            trade.MarkPrice);

        if (TryResolveStructuredGroupIdentity(
                trade.BlockTradeId,
                trade.BlockRfqId,
                trade.ComboTradeId,
                trade.ComboId,
                out groupId,
                out groupType))
        {
            return true;
        }

        if (trade.IsBlockTrade && TryResolveIdentity("trade_id", trade.TradeId, out groupId, out groupType))
        {
            return true;
        }

        if (usdNotional >= minGroupUsd && TryResolveIdentity("trade_id", trade.TradeId, out groupId, out groupType))
        {
            return true;
        }

        groupId = string.Empty;
        groupType = string.Empty;
        return false;
    }

    public static bool TryResolveStructuredGroupIdentity(
        string? blockTradeId,
        string? blockRfqId,
        string? comboTradeId,
        string? comboId,
        out string groupId,
        out string groupType)
    {
        if (TryResolveIdentity("block_trade_id", blockTradeId, out groupId, out groupType) ||
            TryResolveIdentity("block_rfq_id", blockRfqId, out groupId, out groupType) ||
            TryResolveIdentity("combo_trade_id", comboTradeId, out groupId, out groupType) ||
            TryResolveIdentity("combo_id", comboId, out groupId, out groupType))
        {
            return true;
        }

        groupId = string.Empty;
        groupType = string.Empty;
        return false;
    }

    public static decimal EstimateStandaloneTradeUsdNotional(
        InstrumentType instrumentType,
        string baseAsset,
        string quoteAsset,
        string settleAsset,
        decimal quantity,
        decimal? amount,
        decimal price,
        decimal? indexPrice,
        decimal? markPrice)
    {
        if (instrumentType == InstrumentType.Option)
        {
            if (IsUsdLike(settleAsset) || IsUsdLike(quoteAsset))
            {
                return Math.Abs(price * quantity);
            }

            if (IsPriceQuotedInBaseAsset(baseAsset, settleAsset))
            {
                var underlyingPrice = indexPrice ?? markPrice;
                return underlyingPrice is > 0
                    ? Math.Abs(price * quantity * underlyingPrice.Value)
                    : 0m;
            }

            return 0m;
        }

        if (amount is not null && IsUsdLike(quoteAsset))
        {
            return Math.Abs(amount.Value);
        }

        if (IsUsdLike(quoteAsset) || IsUsdLike(settleAsset))
        {
            if (IsPriceQuotedInBaseAsset(baseAsset, settleAsset))
            {
                var referencePrice = indexPrice ?? markPrice;
                return referencePrice is > 0
                    ? Math.Abs((quantity / referencePrice.Value) * price)
                    : 0m;
            }

            return Math.Abs(price * quantity);
        }

        var assetPrice = indexPrice ?? markPrice;
        return assetPrice is > 0
            ? Math.Abs(quantity * assetPrice.Value)
            : 0m;
    }

    private static bool TryResolveIdentity(string type, string? value, out string groupId, out string groupType)
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

    private static bool IsUsdLike(string asset) =>
        asset.Equals("USD", StringComparison.OrdinalIgnoreCase) ||
        asset.Equals("USDT", StringComparison.OrdinalIgnoreCase) ||
        asset.Equals("USDC", StringComparison.OrdinalIgnoreCase) ||
        asset.Equals("FDUSD", StringComparison.OrdinalIgnoreCase);

    private static bool IsPriceQuotedInBaseAsset(string baseAsset, string settleAsset) =>
        settleAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase);
}
