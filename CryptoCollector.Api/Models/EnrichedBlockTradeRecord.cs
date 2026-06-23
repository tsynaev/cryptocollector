using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.Api.Models;

public sealed class EnrichedBlockTradeRecord : ITimeSeriesRecord
{
    public string Exchange { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public InstrumentType InstrumentType { get; set; }
    public string BaseAsset { get; set; } = string.Empty;
    public string QuoteAsset { get; set; } = string.Empty;
    public string SettleAsset { get; set; } = string.Empty;
    public DateTime Date { get; set; }
    public DateTime? ExpiryUtc { get; set; }
    public decimal? StrikePrice { get; set; }
    public string? OptionSide { get; set; }
    public string TradeId { get; set; } = string.Empty;
    public string Side { get; set; } = string.Empty;
    public decimal Quantity { get; set; }
    public decimal? Contracts { get; set; }
    public decimal? Amount { get; set; }
    public decimal Price { get; set; }
    public decimal? MarkPrice { get; set; }
    public decimal? IndexPrice { get; set; }
    public decimal? Iv { get; set; }
    public decimal? MarkIv { get; set; }
    public decimal Notional { get; set; }
    public decimal UsdNotional { get; set; }
    public bool IsBlockTrade { get; set; }
    public string? BlockTradeId { get; set; }
    public int? BlockTradeLegCount { get; set; }
    public string? ComboId { get; set; }
    public string? ComboTradeId { get; set; }
    public string? BlockRfqId { get; set; }
    public string? Liquidation { get; set; }
    public bool IsRpiTrade { get; set; }
    public string? Sequence { get; set; }
    public string GroupId { get; set; } = string.Empty;
    public string GroupType { get; set; } = string.Empty;
    public decimal? PreTradeOpenInterest { get; set; }
    public decimal? PostTradeOpenInterest { get; set; }
    public decimal? OpenInterestDelta =>
        PreTradeOpenInterest is null || PostTradeOpenInterest is null
            ? null
            : PostTradeOpenInterest.Value - PreTradeOpenInterest.Value;
}
