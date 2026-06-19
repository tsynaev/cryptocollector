using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Deribit.Models;
using CryptoCollector.Exchange.Deribit.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Exchange.Deribit.Services;

public sealed class DeribitApiClient(
    HttpClient httpClient,
    IOptions<DeribitCollectorOptions> options,
    ILogger<DeribitApiClient> logger)
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
            .Where(x => MatchesQuoteAsset(x, quoteAsset))
            .Select(MapInstrument)
            .ToArray();
    }

    public Task<IReadOnlyList<DeribitBookSummary>> GetOptionSummariesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetBookSummariesAsync(baseAsset, "option", cancellationToken);

    public async Task<IReadOnlyList<OptionChainSnapshot>> GetOptionChainSnapshotsAsync(CancellationToken cancellationToken)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var summaries = await GetOptionSummariesAsync(_options.BaseAsset, cancellationToken);

        return summaries.Select(summary => new OptionChainSnapshot
        {
            Symbol = summary.InstrumentName,
            Ticker = MapOptionTicker(summary, timestamp)
        }).ToArray();
    }

    public Task<IReadOnlyList<DeribitBookSummary>> GetFutureSummariesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetBookSummariesAsync(baseAsset, "future", cancellationToken);

    public Task<IReadOnlyList<DeribitTrade>> GetRecentOptionTradesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetRecentTradesAsync(baseAsset, "option", cancellationToken);

    public Task<IReadOnlyList<DeribitTrade>> GetRecentFutureTradesAsync(string baseAsset, CancellationToken cancellationToken) =>
        GetRecentTradesAsync(baseAsset, "future", cancellationToken);

    public Task<IReadOnlyList<DeribitTrade>> GetOptionTradesAsync(string baseAsset, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken) =>
        GetTradesByTimeRangeAsync(baseAsset, "option", fromUtc, toUtc, cancellationToken);

    public Task<IReadOnlyList<DeribitTrade>> GetFutureTradesAsync(string baseAsset, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken) =>
        GetTradesByTimeRangeAsync(baseAsset, "future", fromUtc, toUtc, cancellationToken);

    public IAsyncEnumerable<DeribitTrade> StreamOptionTradesAsync(string baseAsset, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken) =>
        StreamTradesByTimeRangeAsync(baseAsset, "option", fromUtc, toUtc, cancellationToken);

    public IAsyncEnumerable<DeribitTrade> StreamFutureTradesAsync(string baseAsset, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken) =>
        StreamTradesByTimeRangeAsync(baseAsset, "future", fromUtc, toUtc, cancellationToken);

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

    private async Task<IReadOnlyList<DeribitTrade>> GetTradesByTimeRangeAsync(
        string currency,
        string kind,
        DateTime fromUtc,
        DateTime toUtc,
        CancellationToken cancellationToken)
    {
        if (toUtc <= fromUtc)
        {
            return [];
        }

        var startTimestamp = new DateTimeOffset(DateTime.SpecifyKind(fromUtc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
        var endTimestamp = new DateTimeOffset(DateTime.SpecifyKind(toUtc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
        var result = new List<DeribitTrade>();

        while (endTimestamp >= startTimestamp)
        {
            var page = await ExecuteWithRetryAsync(
                ct => GetAsync<DeribitTradeBatch>(
                    "public/get_last_trades_by_currency_and_time",
                    new Dictionary<string, object?>
                    {
                        ["currency"] = currency,
                        ["kind"] = kind,
                        ["start_timestamp"] = startTimestamp,
                        ["end_timestamp"] = endTimestamp,
                        ["count"] = _options.RestTradeBootstrapCount,
                        ["sorting"] = "desc"
                    },
                    ct),
                $"public/get_last_trades_by_currency_and_time({kind})",
                cancellationToken);

            if (page.Trades.Count == 0)
            {
                break;
            }

            result.AddRange(page.Trades);

            var oldestTimestamp = page.Trades.Min(static x => x.Timestamp);
            if (!page.HasMore || oldestTimestamp <= startTimestamp)
            {
                break;
            }

            endTimestamp = oldestTimestamp - 1;
        }

        return result;
    }

    private async IAsyncEnumerable<DeribitTrade> StreamTradesByTimeRangeAsync(
        string currency,
        string kind,
        DateTime fromUtc,
        DateTime toUtc,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (toUtc <= fromUtc)
        {
            yield break;
        }

        var startTimestamp = new DateTimeOffset(DateTime.SpecifyKind(fromUtc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
        var endTimestamp = new DateTimeOffset(DateTime.SpecifyKind(toUtc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();

        while (startTimestamp <= endTimestamp)
        {
            var page = await ExecuteWithRetryAsync(
                ct => GetAsync<DeribitTradeBatch>(
                    "public/get_last_trades_by_currency_and_time",
                    new Dictionary<string, object?>
                    {
                        ["currency"] = currency,
                        ["kind"] = kind,
                        ["start_timestamp"] = startTimestamp,
                        ["end_timestamp"] = endTimestamp,
                        ["count"] = _options.RestTradeBootstrapCount,
                        ["sorting"] = "asc"
                    },
                    ct),
                $"public/get_last_trades_by_currency_and_time({kind})",
                cancellationToken);

            if (page.Trades.Count == 0)
            {
                yield break;
            }

            foreach (var trade in page.Trades)
            {
                yield return trade;
            }

            var newestTimestamp = page.Trades.Max(static x => x.Timestamp);
            if (!page.HasMore || newestTimestamp >= endTimestamp)
            {
                yield break;
            }

            startTimestamp = newestTimestamp + 1;
        }
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
        if (!response.IsSuccessStatusCode)
        {
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            logger.LogError("Deribit HTTP request failed. Method={Method}, StatusCode={StatusCode}, Response={Response}.",
                method,
                (int)response.StatusCode,
                responseBody);
            response.EnsureSuccessStatusCode();
        }

        var payload = await response.Content.ReadFromJsonAsync<DeribitRpcResponse<T>>(JsonOptions, cancellationToken);
        if (payload?.Error is not null)
        {
            logger.LogError("Deribit RPC request failed. Method={Method}, Code={Code}, Message={Message}.",
                method,
                payload.Error.Code,
                payload.Error.Message);
            throw new InvalidOperationException($"{method} failed: {payload.Error.Code} {payload.Error.Message}");
        }

        if (payload is null || payload.Result is null)
        {
            logger.LogError("Deribit RPC request returned no result. Method={Method}.", method);
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
                logger.LogWarning(exception, "Deribit REST operation failed. Operation={Operation}, Attempt={Attempt}/{RetryCount}.",
                    operation,
                    attempt,
                    _options.RestRetryCount);
                await Task.Delay(TimeSpan.FromTicks(_options.RestRetryDelay.Ticks * attempt), cancellationToken);
            }
        }

        logger.LogError(lastException, "Deribit REST operation exhausted retries. Operation={Operation}.", operation);
        throw new InvalidOperationException($"{operation} failed.", lastException);
    }

    private static InstrumentDefinition MapInstrument(DeribitInstrument source) =>
        new()
        {
            Exchange = "deribit",
            InstrumentType = source.Kind.Equals("option", StringComparison.OrdinalIgnoreCase)
                ? InstrumentType.Option
                : ResolveDerivativeInstrumentType(source),
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

    private static InstrumentType ResolveDerivativeInstrumentType(DeribitInstrument source)
    {
        if (!source.Kind.Equals("future", StringComparison.OrdinalIgnoreCase))
        {
            return InstrumentType.Unknown;
        }

        var baseAsset = source.BaseCurrency;
        var quoteAsset = source.QuoteCurrency;
        var settlementAsset = source.SettlementCurrency ?? source.BaseCurrency;
        var normalizedInstrumentType = source.InstrumentType?.Trim().ToLowerInvariant();
        var normalizedFutureType = source.FutureType?.Trim().ToLowerInvariant();
        var isPerpetual = source.SettlementPeriod?.Equals("perpetual", StringComparison.OrdinalIgnoreCase) == true;
        var isInverse = normalizedInstrumentType switch
        {
            "reversed" => true,
            "linear" => false,
            _ => normalizedFutureType switch
            {
                "reversed" => true,
                "linear" => false,
                _ => settlementAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase) &&
                     !quoteAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase)
            }
        };

        if (isPerpetual)
        {
            return isInverse ? InstrumentType.InversePerpetual : InstrumentType.LinearPerpetual;
        }

        return isInverse ? InstrumentType.InverseFutures : InstrumentType.LinearFutures;
    }

    private static bool MatchesQuoteAsset(DeribitInstrument source, string quoteAsset)
    {
        if (source.QuoteCurrency.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return !string.IsNullOrWhiteSpace(source.CounterCurrency) &&
               source.CounterCurrency.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase);
    }

    public static ExchangeTicker MapFutureTicker(DeribitBookSummary summary, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            LastPrice = summary.Last,
            MarkPrice = summary.MarkPrice,
            BidPrice = summary.BidPrice,
            AskPrice = summary.AskPrice,
            OpenInterest = summary.OpenInterest,
            Volume24h = summary.Volume
        };

    public static ExchangeOptionTicker MapOptionTicker(DeribitBookSummary summary, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            BidPrice = summary.BidPrice,
            AskPrice = summary.AskPrice,
            LastPrice = summary.Last,
            MarkPrice = summary.MarkPrice,
            UnderlyingPrice = summary.UnderlyingPrice,
            OpenInterest = summary.OpenInterest,
            Volume24h = summary.Volume,
            Change24h = summary.PriceChange
        };
}
