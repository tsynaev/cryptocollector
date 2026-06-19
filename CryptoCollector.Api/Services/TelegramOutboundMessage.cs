namespace CryptoCollector.Api.Services;

public sealed record OutboundMessage(
    string Caption,
    byte[]? PhotoBytes = null,
    string? PhotoFileName = null);
