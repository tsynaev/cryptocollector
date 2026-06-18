namespace CryptoCollector.API.Exchange.Models;

public sealed class ExchangeTrade
{
    public DateTimeOffset TradeTime { get; init; }
    public string Symbol { get; init; } = string.Empty;
    public string Side { get; init; } = string.Empty;
    public decimal Quantity { get; init; }
    public decimal Price { get; init; }
    public string TradeId { get; init; } = string.Empty;
    public bool IsBlockTrade { get; init; }
    public string? BlockTradeId { get; init; }
    public bool IsRpiTrade { get; init; }
    public string? Sequence { get; init; }
}
