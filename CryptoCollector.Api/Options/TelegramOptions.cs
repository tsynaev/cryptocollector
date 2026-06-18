namespace CryptoCollector.Api.Options;

public sealed class TelegramOptions
{
    public const string SectionName = "Telegram";

    public bool Enabled { get; init; }
    public string BotToken { get; init; } = string.Empty;
    public string ChatId { get; init; } = string.Empty;
    public int QueueCapacity { get; init; } = 1000;
}
