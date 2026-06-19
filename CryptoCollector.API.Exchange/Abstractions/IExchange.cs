using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.API.Exchange.Abstractions;

public interface IExchange
{
    string Name { get; }
    TimeSpan ReconnectDelay { get; }
    TimeSpan OptionChainSnapshotInterval { get; }
    Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(CancellationToken cancellationToken);
    IAsyncEnumerable<ExchangeTradeMessage> StreamTradesSinceAsync(IReadOnlyList<InstrumentDefinition> instruments, DateTime? catchUpFromUtc, CancellationToken cancellationToken);
    IAsyncEnumerable<ExchangeTickerMessage> StreamTickersSnapshotAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken);
    IAsyncEnumerable<ExchangeOptionMessage> StreamOptionChainSnapshotAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken);
    IAsyncEnumerable<ExchangeDataMessage> StreamAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken);
}
