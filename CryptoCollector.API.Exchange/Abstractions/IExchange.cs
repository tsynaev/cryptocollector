using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;

namespace CryptoCollector.API.Exchange.Abstractions;

public interface IExchange
{
    string Name { get; }
    TimeSpan ReconnectDelay { get; }
    TimeSpan OptionChainSnapshotInterval { get; }
    Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(CancellationToken cancellationToken);
    Task BootstrapAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken);
    Task StreamAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken);
    Task PollOptionChainSnapshotsAsync(IReadOnlyList<InstrumentDefinition> instruments, IMarketDataSink sink, CancellationToken cancellationToken);
}
