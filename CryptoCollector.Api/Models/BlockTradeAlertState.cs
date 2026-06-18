namespace CryptoCollector.Api.Models;

public sealed class BlockTradeAlertState
{
    public Dictionary<string, AlertCursor> Exchanges { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class AlertCursor
{
    public DateTime LastCheckedTradeUtc { get; init; }
    public string LastCheckedTradeKey { get; init; } = string.Empty;
}
