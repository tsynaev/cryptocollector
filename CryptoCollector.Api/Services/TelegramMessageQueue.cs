using System.Globalization;
using System.Reflection;
using System.Text.Encodings.Web;
using System.Threading.Channels;
using CryptoCollector.Api.Options;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Api.Services;

public sealed class TelegramMessageQueue : BackgroundService, IMessageQueue
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly TelegramOptions _options;
    private readonly ILogger<TelegramMessageQueue> _logger;
    private readonly Channel<string> _channel;
    private int _startupMessageQueued;

    public TelegramMessageQueue(
        IHttpClientFactory httpClientFactory,
        IOptions<TelegramOptions> options,
        IHostApplicationLifetime applicationLifetime,
        ILogger<TelegramMessageQueue> logger)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        _logger = logger;
        _channel = Channel.CreateBounded<string>(new BoundedChannelOptions(Math.Max(1, _options.QueueCapacity))
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });

        applicationLifetime.ApplicationStarted.Register(EnqueueStartupMessage);
    }

    public bool TryEnqueue(string message)
    {
        if (!_options.Enabled || string.IsNullOrWhiteSpace(_options.BotToken) || string.IsNullOrWhiteSpace(_options.ChatId))
        {
            _logger.LogWarning(
                "Telegram message skipped. Enabled={Enabled}, HasBotToken={HasBotToken}, HasChatId={HasChatId}.",
                _options.Enabled,
                !string.IsNullOrWhiteSpace(_options.BotToken),
                !string.IsNullOrWhiteSpace(_options.ChatId));
            return false;
        }

        return _channel.Writer.TryWrite(message);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled || string.IsNullOrWhiteSpace(_options.BotToken) || string.IsNullOrWhiteSpace(_options.ChatId))
        {
            _logger.LogWarning(
                "Telegram sender is not started. Enabled={Enabled}, HasBotToken={HasBotToken}, HasChatId={HasChatId}.",
                _options.Enabled,
                !string.IsNullOrWhiteSpace(_options.BotToken),
                !string.IsNullOrWhiteSpace(_options.ChatId));
            return;
        }

        _logger.LogInformation("Telegram sender started.");

        var client = _httpClientFactory.CreateClient(nameof(TelegramMessageQueue));
        var endpoint = $"https://api.telegram.org/bot{_options.BotToken}/sendMessage";

        await foreach (var message in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                var response = await client.PostAsJsonAsync(endpoint, new
                {
                    chat_id = _options.ChatId,
                    text = message,
                    parse_mode = "HTML"
                }, cancellationToken: stoppingToken);

                if (!response.IsSuccessStatusCode)
                {
                    var body = await response.Content.ReadAsStringAsync(stoppingToken);
                    _logger.LogWarning("Telegram send failed. StatusCode={StatusCode}, Response={Response}.", (int)response.StatusCode, body);
                }
                else
                {
                    _logger.LogInformation("Telegram message sent successfully.");
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Telegram message send failed.");
            }
        }
    }

    private void EnqueueStartupMessage()
    {
        if (Interlocked.Exchange(ref _startupMessageQueued, 1) != 0)
        {
            return;
        }

        if (!TryEnqueue(BuildStartupMessage()))
        {
            _logger.LogWarning("Failed to enqueue Telegram startup message.");
        }
        else
        {
            _logger.LogInformation("Telegram startup message enqueued.");
        }
    }

    private static string BuildStartupMessage()
    {
        var assembly = Assembly.GetEntryAssembly();
        var version = assembly?.GetName().Version?.ToString() ?? "unknown";
        var machineName = HtmlEncoder.Default.Encode(Environment.MachineName);
        var startedAt = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);

        return string.Join('\n', [
            "<b>CryptoCollector started</b>",
            $"host=<code>{machineName}</code>",
            $"version=<code>{HtmlEncoder.Default.Encode(version)}</code>"
        ]);
    }
}
