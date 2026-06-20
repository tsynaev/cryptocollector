using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.Api.Models;

public sealed class BlockTradeHistoryGroup
{
    public string Exchange { get; init; } = string.Empty;
    public string GroupId { get; init; } = string.Empty;
    public string GroupType { get; init; } = string.Empty;
    public DateTime TimestampUtc { get; init; }
    public string? BaseAsset { get; init; }
    public decimal TotalQuantity { get; init; }
    public decimal TotalUsdNotional { get; init; }
    public int LegCount { get; init; }
    public IReadOnlyList<BlockTradeHistoryLeg> Legs { get; init; } = [];
}

public sealed class BlockTradeHistoryLeg
{
    public string Symbol { get; init; } = string.Empty;
    public InstrumentType InstrumentType { get; init; }
    public string BaseAsset { get; init; } = string.Empty;
    public string QuoteAsset { get; init; } = string.Empty;
    public string SettleAsset { get; init; } = string.Empty;
    public DateTime Date { get; init; }
    public DateTime? ExpiryUtc { get; init; }
    public decimal? StrikePrice { get; init; }
    public string? OptionSide { get; init; }
    public string TradeId { get; init; } = string.Empty;
    public string Side { get; init; } = string.Empty;
    public decimal Quantity { get; init; }
    public decimal? Contracts { get; init; }
    public decimal? Amount { get; init; }
    public decimal Price { get; init; }
    public decimal? MarkPrice { get; init; }
    public decimal? IndexPrice { get; init; }
    public decimal? Iv { get; init; }
    public decimal? MarkIv { get; init; }
    public decimal UsdNotional { get; init; }
    public bool IsBlockTrade { get; init; }
    public string? BlockTradeId { get; init; }
    public int? BlockTradeLegCount { get; init; }
    public string? ComboId { get; init; }
    public string? ComboTradeId { get; init; }
    public string? BlockRfqId { get; init; }
    public string? Liquidation { get; init; }
    public bool IsRpiTrade { get; init; }
    public string? Sequence { get; init; }
}
