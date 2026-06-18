namespace CryptoCollector.API.Exchange.Models;

public sealed class ExchangeBootstrapBatch
{
    public IReadOnlyList<ExchangeTradeMessage> Trades { get; init; } = [];
    public IReadOnlyList<ExchangeTickerMessage> Tickers { get; init; } = [];
    public IReadOnlyList<ExchangeOptionMessage> Options { get; init; } = [];
}
