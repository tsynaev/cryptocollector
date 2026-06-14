using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Deribit.Models;
using CryptoCollector.Exchange.Deribit.Options;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Exchange.Deribit.Services;

public sealed class DeribitApiClient(HttpClient httpClient, IOptions<DeribitCollectorOptions> options) : IExchangeMarketDataClient
{
    private readonly DeribitCollectorOptions _options = options.Value;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public string Exchange => "deribit";

    public async Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(string baseAsset, string quoteAsset, CancellationToken cancellationToken)
    {
        var optionsInstruments = await GetInstrumentsAsync(baseAsset, "option", cancellationToken);
        var futureInstruments = await GetInstrumentsAsync(baseAsset, "future", cancellationToken);

        return optionsInstruments
            .Concat(futureInstruments)
            .Where(x => x.IsActive)
            .Where(x => x.BaseCurrency.Equals(baseAsset, StringComparison.OrdinalIgnoreCase))
            .Where(x => x.QuoteCurrency.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
            .Select(MapInstrument)
            .ToArray();
    }

    public Task<IReadOnlyList<DeribitBookSummary>> GetOptionSummariesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetBookSummariesAsync(baseAsset, "option", cancellationToken);

    public Task<IReadOnlyList<DeribitBookSummary>> GetFutureSummariesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetBookSummariesAsync(baseAsset, "future", cancellationToken);

    public Task<IReadOnlyList<DeribitTrade>> GetRecentOptionTradesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetRecentTradesAsync(baseAsset, "option", cancellationToken);

    public Task<IReadOnlyList<DeribitTrade>> GetRecentFutureTradesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetRecentTradesAsync(baseAsset, "future", cancellationToken);

    private async Task<IReadOnlyList<DeribitInstrument>> GetInstrumentsAsync(string currency, string kind, CancellationToken cancellationToken)
    {
        var response = await ExecuteWithRetryAsync(
            ct => GetAsync<IReadOnlyList<DeribitInstrument>>(
                "public/get_instruments",
                new Dictionary<string, object?>
                {
                    ["currency"] = currency,
                    ["kind"] = kind
                },
                ct),
            $"public/get_instruments({kind})",
            cancellationToken);

        return response;
    }

    private async Task<IReadOnlyList<DeribitBookSummary>> GetBookSummariesAsync(string currency, string kind, CancellationToken cancellationToken)
    {
        var response = await ExecuteWithRetryAsync(
            ct => GetAsync<IReadOnlyList<DeribitBookSummary>>(
                "public/get_book_summary_by_currency",
                new Dictionary<string, object?>
                {
                    ["currency"] = currency,
                    ["kind"] = kind
                },
                ct),
            $"public/get_book_summary_by_currency({kind})",
            cancellationToken);

        return response;
    }

    private async Task<IReadOnlyList<DeribitTrade>> GetRecentTradesAsync(string currency, string kind, CancellationToken cancellationToken)
    {
        var response = await ExecuteWithRetryAsync(
            ct => GetAsync<DeribitTradeBatch>(
                "public/get_last_trades_by_currency",
                new Dictionary<string, object?>
                {
                    ["currency"] = currency,
                    ["kind"] = kind,
                    ["count"] = _options.RestTradeBootstrapCount,
                    ["sorting"] = "desc"
                },
                ct),
            $"public/get_last_trades_by_currency({kind})",
            cancellationToken);

        return response.Trades;
    }

    private async Task<T> GetAsync<T>(string method, Dictionary<string, object?> parameters, CancellationToken cancellationToken)
        where T : notnull
    {
        var request = new
        {
            jsonrpc = "2.0",
            id = 1,
            method,
            @params = parameters
        };

        using var response = await httpClient.PostAsJsonAsync(string.Empty, request, JsonOptions, cancellationToken);
        response.EnsureSuccessStatusCode();

        var payload = await response.Content.ReadFromJsonAsync<DeribitRpcResponse<T>>(JsonOptions, cancellationToken);
        if (payload?.Error is not null)
        {
            throw new InvalidOperationException($"{method} failed: {payload.Error.Code} {payload.Error.Message}");
        }

        if (payload is null || payload.Result is null)
        {
            throw new InvalidOperationException($"{method} returned no result.");
        }

        return payload.Result;
    }

    private async Task<T> ExecuteWithRetryAsync<T>(
        Func<CancellationToken, Task<T>> action,
        string operation,
        CancellationToken cancellationToken)
        where T : notnull
    {
        Exception? lastException = null;

        for (var attempt = 1; attempt <= _options.RestRetryCount; attempt++)
        {
            try
            {
                return await action(cancellationToken);
            }
            catch (Exception exception) when (attempt < _options.RestRetryCount)
            {
                lastException = exception;
                await Task.Delay(TimeSpan.FromTicks(_options.RestRetryDelay.Ticks * attempt), cancellationToken);
            }
        }

        throw new InvalidOperationException($"{operation} failed.", lastException);
    }

    private static InstrumentDefinition MapInstrument(DeribitInstrument source) =>
        new()
        {
            Exchange = "deribit",
            Category = source.Kind,
            MarketType = source.Kind == "future" && source.SettlementPeriod?.Equals("perpetual", StringComparison.OrdinalIgnoreCase) == true
                ? "perpetual"
                : source.Kind,
            Symbol = source.InstrumentName,
            BaseAsset = source.BaseCurrency,
            QuoteAsset = source.QuoteCurrency,
            SettleAsset = source.SettlementCurrency ?? source.BaseCurrency,
            ExpiryUtc = DateTimeOffset.FromUnixTimeMilliseconds(source.ExpirationTimestamp).UtcDateTime,
            StrikePrice = source.Strike,
            OptionSide = source.OptionType is null
                ? null
                : source.OptionType.Equals("call", StringComparison.OrdinalIgnoreCase) ? "Call" : "Put"
        };
}
