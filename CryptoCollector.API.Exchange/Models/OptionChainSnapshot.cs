namespace CryptoCollector.API.Exchange.Models;

public sealed class OptionChainSnapshot
{
    public required string Symbol { get; init; }
    public required ExchangeOptionTicker Ticker { get; init; }
}
