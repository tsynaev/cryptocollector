namespace CryptoCollector.Api.Services;

public interface ILocalMessageBus
{
    void Publish<T>(T message) where T : class;
    IDisposable Subscribe<T>(Func<T, Task> handler) where T : class;
}
