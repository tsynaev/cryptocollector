namespace CryptoCollector.Api.Options;

public sealed class AggregationOptions
{
    public const string SectionName = "Aggregation";

    public decimal MinTradeQuantity { get; init; } = 0m;
}
