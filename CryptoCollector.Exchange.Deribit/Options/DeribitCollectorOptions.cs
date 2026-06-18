namespace CryptoCollector.Exchange.Deribit.Options;

public sealed class DeribitCollectorOptions
{
    public const string SectionName = "Deribit";

    public string BaseAsset { get; init; } = "BTC";
    public string QuoteAsset { get; init; } = "USD";
    public string RestBaseUrl { get; init; } = "https://www.deribit.com/api/v2/";
    public string WebSocketUrl { get; init; } = "wss://www.deribit.com/ws/api/v2";
    public string TickerInterval { get; init; } = "agg2";
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan InstrumentRefreshInterval { get; init; } = TimeSpan.FromMinutes(30);
    public TimeSpan ReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan RestRetryDelay { get; init; } = TimeSpan.FromSeconds(3);
    public int RestRetryCount { get; init; } = 5;
    public int RestTradeBootstrapCount { get; init; } = 1000;
    public int SubscriptionChunkSize { get; init; } = 500;
    public int WebSocketReceiveBufferSize { get; init; } = 1024 * 128;
}
