namespace CryptoCollector.Api.Options;

public sealed class TelegramOptions
{
    public const string SectionName = "Telegram";

    public bool Enabled { get; set; }
    public string BotToken { get; set; } = string.Empty;
    public string ChatId { get; set; } = string.Empty;
    public int QueueCapacity { get; set; } = 1000;
}
