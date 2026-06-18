namespace CryptoCollector.Api.Services;

public interface IMessageQueue
{
    bool TryEnqueue(string message);
}
