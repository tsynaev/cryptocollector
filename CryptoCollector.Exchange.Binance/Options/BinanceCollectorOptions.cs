namespace CryptoCollector.Exchange.Binance.Options;

public sealed class BinanceCollectorOptions
{
    public const string SectionName = "Binance";

    public string BaseAsset { get; init; } = "BTC";
    public string QuoteAsset { get; init; } = "USDT";
    public string OptionsRestBaseUrl { get; init; } = "https://eapi.binance.com/";
    public string OptionsWebSocketBaseUrl { get; init; } = "wss://fstream.binance.com";
    public TimeSpan InstrumentRefreshInterval { get; init; } = TimeSpan.FromMinutes(30);
    public TimeSpan ReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan RestRetryDelay { get; init; } = TimeSpan.FromSeconds(3);
    public int RestRetryCount { get; init; } = 5;
    public int RecentTradesLimit { get; init; } = 1000;
    public int OptionBootstrapSymbolLimit { get; init; } = 50;
    public int FuturesSubscriptionChunkSize { get; init; } = 100;
    public int OptionTradeStreamsPerConnection { get; init; } = 180;
    public int OptionTradeSubscribeBatchSize { get; init; } = 50;
    public TimeSpan OptionTradeSubscribeMessageDelay { get; init; } = TimeSpan.FromMilliseconds(150);
    public int WebSocketReceiveBufferSize { get; init; } = 1024 * 64;

    public string UnderlyingSymbol => $"{BaseAsset}{QuoteAsset}";
}
