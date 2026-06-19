namespace CryptoCollector.API.Exchange.Models;

public sealed class ExchangeTrade
{
    public DateTimeOffset TradeTime { get; init; }
    public string Symbol { get; init; } = string.Empty;
    public string Side { get; init; } = string.Empty;
    public decimal Quantity { get; init; }
    public decimal? Contracts { get; init; }
    public decimal? Amount { get; init; }
    public decimal Price { get; init; }
    public decimal? MarkPrice { get; init; }
    public decimal? IndexPrice { get; init; }
    public decimal? Iv { get; init; }
    public decimal? MarkIv { get; init; }
    public string? TickDirection { get; init; }
    public string TradeId { get; init; } = string.Empty;
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
