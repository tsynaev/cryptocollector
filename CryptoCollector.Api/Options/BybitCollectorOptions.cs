namespace CryptoCollector.Api.Options;

public sealed class BybitCollectorOptions
{
    public const string SectionName = "Bybit";

    public string BaseAsset { get; init; } = "BTC";
    public string QuoteAsset { get; init; } = "USDT";
    public TimeSpan InstrumentRefreshInterval { get; init; } = TimeSpan.FromMinutes(30);
    public TimeSpan ReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(20);
    public int OptionTickerChunkSize { get; init; } = 250;
    public int LinearChunkSize { get; init; } = 100;
    public decimal MinTradeQuantity { get; init; } = 0m;
    public int RestRetryCount { get; init; } = 5;
    public TimeSpan RestRetryDelay { get; init; } = TimeSpan.FromSeconds(3);
}
