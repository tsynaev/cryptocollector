using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.API.Exchange.Abstractions;

public interface IExchangeMarketDataClient
{
    string Exchange { get; }
    Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(string baseAsset, string quoteAsset, CancellationToken cancellationToken);
}
