namespace CryptoCollector.API.Exchange.Models;

public sealed class ExchangeTrade
{
    public long TradeTime { get; init; }
    public string Symbol { get; init; } = string.Empty;
    public string Side { get; init; } = string.Empty;
    public string Size { get; init; } = string.Empty;
    public string Price { get; init; } = string.Empty;
    public string TradeId { get; init; } = string.Empty;
    public bool IsBlockTrade { get; init; }
    public string? BlockTradeId { get; init; }
    public bool IsRpiTrade { get; init; }
    public string? Sequence { get; init; }
}
