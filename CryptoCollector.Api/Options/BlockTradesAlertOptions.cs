namespace CryptoCollector.Api.Options;

public sealed class BlockTradesAlertOptions
{
    public const string SectionName = "BlockTradesAlert";

    public bool Enabled { get; init; }
    public decimal QuantityThreshold { get; init; } = 500m;
    public TimeSpan GroupWindow { get; init; } = TimeSpan.FromSeconds(2);
}
