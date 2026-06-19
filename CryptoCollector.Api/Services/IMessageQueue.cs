namespace CryptoCollector.Api.Services;

public interface IMessageQueue
{
    bool TryEnqueue(OutboundMessage message);
}
