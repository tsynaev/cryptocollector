namespace CryptoCollector.API.Exchange.Models;

public sealed class InstrumentDefinition
{
    public required string Exchange { get; init; }
    public required string Category { get; init; }
    public required string MarketType { get; init; }
    public required string Symbol { get; init; }
    public required string BaseAsset { get; init; }
    public required string QuoteAsset { get; init; }
    public required string SettleAsset { get; init; }
    public DateTime? ExpiryUtc { get; init; }
    public decimal? StrikePrice { get; init; }
    public string? OptionSide { get; init; }
}
