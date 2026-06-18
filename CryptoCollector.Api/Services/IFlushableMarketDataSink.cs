namespace CryptoCollector.Api.Services;

public interface IFlushableMarketDataSink
{
    Task FlushPendingAsync(CancellationToken cancellationToken);
}
