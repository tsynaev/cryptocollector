using System.Globalization;
using System.Net.Http.Headers;
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
    private readonly Channel<OutboundMessage> _channel;
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
        _channel = Channel.CreateBounded<OutboundMessage>(new BoundedChannelOptions(Math.Max(1, _options.QueueCapacity))
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });

        applicationLifetime.ApplicationStarted.Register(EnqueueStartupMessage);
    }

    public bool TryEnqueue(OutboundMessage message)
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
        await foreach (var message in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                using var response = message.PhotoBytes is null
                    ? await SendTextMessageAsync(client, message, stoppingToken)
                    : await SendPhotoMessageAsync(client, message, stoppingToken);

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

        if (!TryEnqueue(new OutboundMessage(BuildStartupMessage())))
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

        return string.Join('\n', [
            "<b>CryptoCollector started</b>",
            $"host=<code>{machineName}</code>",
            $"version=<code>{HtmlEncoder.Default.Encode(version)}</code>"
        ]);
    }

    private Task<HttpResponseMessage> SendTextMessageAsync(
        HttpClient client,
        OutboundMessage message,
        CancellationToken cancellationToken)
    {
        var endpoint = $"https://api.telegram.org/bot{_options.BotToken}/sendMessage";

        return client.PostAsJsonAsync(endpoint, new
        {
            chat_id = _options.ChatId,
            text = message.Caption,
            parse_mode = "HTML"
        }, cancellationToken: cancellationToken);
    }

    private async Task<HttpResponseMessage> SendPhotoMessageAsync(
        HttpClient client,
        OutboundMessage message,
        CancellationToken cancellationToken)
    {
        var endpoint = $"https://api.telegram.org/bot{_options.BotToken}/sendPhoto";
        using var content = new MultipartFormDataContent();
        content.Add(new StringContent(_options.ChatId), "chat_id");
        content.Add(new StringContent(message.Caption), "caption");
        content.Add(new StringContent("HTML"), "parse_mode");

        using var photoContent = new ByteArrayContent(message.PhotoBytes!);
        photoContent.Headers.ContentType = new MediaTypeHeaderValue("image/png");
        content.Add(photoContent, "photo", message.PhotoFileName ?? "chart.png");

        return await client.PostAsync(endpoint, content, cancellationToken);
    }
}
