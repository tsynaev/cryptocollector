using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.API.Exchange.Abstractions;

public interface IExchange
{
    string Name { get; }
    TimeSpan ReconnectDelay { get; }
    TimeSpan OptionChainSnapshotInterval { get; }
    Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(CancellationToken cancellationToken);
    Task<ExchangeBootstrapBatch> BootstrapAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken);
    IAsyncEnumerable<ExchangeDataMessage> StreamAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken);
    Task<IReadOnlyList<ExchangeOptionMessage>> PollOptionChainSnapshotsAsync(IReadOnlyList<InstrumentDefinition> instruments, CancellationToken cancellationToken);
}
