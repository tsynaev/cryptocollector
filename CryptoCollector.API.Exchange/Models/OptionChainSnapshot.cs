using System.Text.Json;

namespace CryptoCollector.API.Exchange.Models;

public sealed class OptionChainSnapshot
{
    public required string Symbol { get; init; }
    public required JsonElement Payload { get; init; }
    public required DateTimeOffset TimestampUtc { get; init; }
}
