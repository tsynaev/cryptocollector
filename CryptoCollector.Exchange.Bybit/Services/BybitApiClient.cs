using Bybit.Net.Clients;
using Bybit.Net.Enums;
using Bybit.Net.Objects.Models.V5;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Bybit.Options;
using CryptoExchange.Net.Objects;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text.RegularExpressions;

namespace CryptoCollector.Exchange.Bybit.Services;

public sealed class BybitApiClient(BybitRestClient restClient, IOptions<BybitCollectorOptions> options) : IExchangeMarketDataClient
{
    private static readonly Regex OptionSymbolRegex = new(
        "^(?<base>[A-Z]+)-(?<expiry>\\d{1,2}[A-Z]{3}\\d{2})-(?<strike>\\d+(?:\\.\\d+)?)-(?<type>[CP])(?:-(?<settle>[A-Z]+))?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private readonly BybitCollectorOptions _options = options.Value;

    public string Exchange => "bybit";

    public async Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(string baseAsset, string quoteAsset, CancellationToken cancellationToken)
    {
        var optionSource = await GetOptionSymbolsAsync(baseAsset, cancellationToken);
        var linearSource = await GetLinearSymbolsAsync(baseAsset, cancellationToken);

        var options = optionSource
            .Where(x => x.BaseAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase))
            .Where(x => x.QuoteAsset.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
            .Select(MapOptionInstrument);

        var linear = linearSource
            .Where(x => x.BaseAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase))
            .Where(x => x.QuoteAsset.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
            .Select(MapLinearInstrument);

        return options.Concat(linear).ToArray();
    }

    public async Task<IReadOnlyList<BybitOptionTicker>> GetOptionTickersAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var result = await ExecuteWithRetryAsync(
            ct => restClient.V5Api.ExchangeData.GetOptionTickersAsync(
                symbol: string.Empty,
                baseAsset: baseAsset,
                expirationDate: null,
                ct: ct),
            "GetOptionTickersAsync",
            cancellationToken);

        return GetData(result, "GetOptionTickersAsync").List;
    }

    public async Task<IReadOnlyList<BybitLinearInverseTicker>> GetLinearTickersAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var result = await ExecuteWithRetryAsync(
            ct => restClient.V5Api.ExchangeData.GetLinearInverseTickersAsync(
                Category.Linear,
                symbol: string.Empty,
                baseAsset: baseAsset,
                expirationDate: null,
                ct: ct),
            "GetLinearInverseTickersAsync",
            cancellationToken);

        return GetData(result, "GetLinearInverseTickersAsync").List;
    }

    public async Task<IReadOnlyList<BybitTradeHistory>> GetRecentOptionTradesAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var result = await ExecuteWithRetryAsync(
            ct => restClient.V5Api.ExchangeData.GetTradeHistoryAsync(
                Category.Option,
                symbol: string.Empty,
                baseAsset: baseAsset,
                optionType: null,
                limit: 1000,
                ct: ct),
            "GetTradeHistoryAsync(option)",
            cancellationToken);

        return GetData(result, "GetTradeHistoryAsync(option)").List;
    }

    public async Task<IReadOnlyList<BybitTradeHistory>> GetRecentLinearTradesAsync(string symbol, CancellationToken cancellationToken)
    {
        var result = await ExecuteWithRetryAsync(
            ct => restClient.V5Api.ExchangeData.GetTradeHistoryAsync(
                Category.Linear,
                symbol: symbol,
                baseAsset: string.Empty,
                optionType: null,
                limit: 1000,
                ct: ct),
            $"GetTradeHistoryAsync({symbol})",
            cancellationToken);

        return GetData(result, $"GetTradeHistoryAsync({symbol})").List;
    }

    private async Task<IReadOnlyList<BybitOptionSymbol>> GetOptionSymbolsAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var results = new List<BybitOptionSymbol>();
        string? cursor = null;

        do
        {
            var result = await ExecuteWithRetryAsync(
                ct => restClient.V5Api.ExchangeData.GetOptionSymbolsAsync(
                    symbol: null,
                    baseAsset: baseAsset,
                    limit: 1000,
                    cursor: cursor,
                    ct: ct),
                "GetOptionSymbolsAsync",
                cancellationToken);

            var data = GetData(result, "GetOptionSymbolsAsync");
            results.AddRange(data.List);
            cursor = string.IsNullOrWhiteSpace(data.NextPageCursor) ? null : data.NextPageCursor;
        }
        while (cursor is not null);

        return results;
    }

    private async Task<IReadOnlyList<BybitLinearInverseSymbol>> GetLinearSymbolsAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var results = new List<BybitLinearInverseSymbol>();
        string? cursor = null;

        do
        {
            var result = await ExecuteWithRetryAsync(
                ct => restClient.V5Api.ExchangeData.GetLinearInverseSymbolsAsync(
                    Category.Linear,
                    symbol: null,
                    baseAsset: baseAsset,
                    status: null,
                    symbolType: null,
                    limit: 1000,
                    cursor: cursor,
                    ct: ct),
                "GetLinearInverseSymbolsAsync",
                cancellationToken);

            var data = GetData(result, "GetLinearInverseSymbolsAsync");
            results.AddRange(data.List);
            cursor = string.IsNullOrWhiteSpace(data.NextPageCursor) ? null : data.NextPageCursor;
        }
        while (cursor is not null);

        return results;
    }

    private static BybitResponse<T> GetData<T>(WebCallResult<BybitResponse<T>> result, string operation)
    {
        if (!result.Success || result.Data is null)
        {
            throw new InvalidOperationException($"{operation} failed: {result.Error}");
        }

        return result.Data;
    }

    private async Task<WebCallResult<BybitResponse<T>>> ExecuteWithRetryAsync<T>(
        Func<CancellationToken, Task<WebCallResult<BybitResponse<T>>>> action,
        string operation,
        CancellationToken cancellationToken)
    {
        WebCallResult<BybitResponse<T>>? lastResult = null;

        for (var attempt = 1; attempt <= _options.RestRetryCount; attempt++)
        {
            lastResult = await action(cancellationToken);
            if (lastResult.Success && lastResult.Data is not null)
            {
                return lastResult;
            }

            if (attempt == _options.RestRetryCount)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromTicks(_options.RestRetryDelay.Ticks * attempt), cancellationToken);
        }

        throw new InvalidOperationException($"{operation} failed: {lastResult?.Error}");
    }

    private static InstrumentDefinition MapOptionInstrument(BybitOptionSymbol source) =>
        new()
        {
            Exchange = "bybit",
            Category = "option",
            MarketType = "option",
            Symbol = source.Name,
            BaseAsset = source.BaseAsset,
            QuoteAsset = source.QuoteAsset,
            SettleAsset = source.SettleAsset,
            ExpiryUtc = source.DeliveryTime,
            StrikePrice = TryParseOptionStrike(source.Name),
            OptionSide = source.OptionType == OptionType.Call ? "Call" : "Put"
        };

    private static InstrumentDefinition MapLinearInstrument(BybitLinearInverseSymbol source) =>
        new()
        {
            Exchange = "bybit",
            Category = "linear",
            MarketType = source.ContractType.ToString().Contains("Perpetual", StringComparison.OrdinalIgnoreCase) ? "perpetual" : "future",
            Symbol = source.Name,
            BaseAsset = source.BaseAsset,
            QuoteAsset = source.QuoteAsset,
            SettleAsset = source.SettleAsset,
            ExpiryUtc = source.DeliveryTime,
            StrikePrice = null,
            OptionSide = null
        };

    private static decimal? TryParseOptionStrike(string symbol)
    {
        var optionMatch = OptionSymbolRegex.Match(symbol);
        if (!optionMatch.Success)
        {
            return null;
        }

        return decimal.TryParse(optionMatch.Groups["strike"].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var strikeValue)
            ? strikeValue
            : null;
    }
}
