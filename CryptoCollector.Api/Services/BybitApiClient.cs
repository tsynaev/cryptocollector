using Bybit.Net.Clients;
using Bybit.Net.Enums;
using Bybit.Net.Objects.Models.V5;
using CryptoCollector.Api.Models;
using CryptoExchange.Net.Objects;

namespace CryptoCollector.Api.Services;

public sealed class BybitApiClient(BybitRestClient restClient)
{
    public async Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(string baseAsset, string quoteAsset, CancellationToken cancellationToken)
    {
        var optionSource = await GetOptionSymbolsAsync(baseAsset, cancellationToken);
        var linearSource = await GetLinearSymbolsAsync(baseAsset, cancellationToken);

        var options = optionSource
            .Where(x => x.BaseAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase))
            .Where(x => x.QuoteAsset.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
            .Select(InstrumentDefinition.FromBybit);

        var linear = linearSource
            .Where(x => x.BaseAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase))
            .Where(x => x.QuoteAsset.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
            .Select(InstrumentDefinition.FromBybit);

        return options.Concat(linear).ToArray();
    }

    public async Task<IReadOnlyList<BybitOptionTicker>> GetOptionTickersAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var result = await restClient.V5Api.ExchangeData.GetOptionTickersAsync(
            symbol: string.Empty,
            baseAsset: baseAsset,
            expirationDate: null,
            ct: cancellationToken);

        return GetData(result, "GetOptionTickersAsync").List;
    }

    public async Task<IReadOnlyList<BybitLinearInverseTicker>> GetLinearTickersAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var result = await restClient.V5Api.ExchangeData.GetLinearInverseTickersAsync(
            Category.Linear,
            symbol: string.Empty,
            baseAsset: baseAsset,
            expirationDate: null,
            ct: cancellationToken);

        return GetData(result, "GetLinearInverseTickersAsync").List;
    }

    public async Task<IReadOnlyList<BybitTradeHistory>> GetRecentOptionTradesAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var result = await restClient.V5Api.ExchangeData.GetTradeHistoryAsync(
            Category.Option,
            symbol: string.Empty,
            baseAsset: baseAsset,
            optionType: null,
            limit: 1000,
            ct: cancellationToken);

        return GetData(result, "GetTradeHistoryAsync(option)").List;
    }

    public async Task<IReadOnlyList<BybitTradeHistory>> GetRecentLinearTradesAsync(string symbol, CancellationToken cancellationToken)
    {
        var result = await restClient.V5Api.ExchangeData.GetTradeHistoryAsync(
            Category.Linear,
            symbol: symbol,
            baseAsset: string.Empty,
            optionType: null,
            limit: 1000,
            ct: cancellationToken);

        return GetData(result, $"GetTradeHistoryAsync({symbol})").List;
    }

    private async Task<IReadOnlyList<BybitOptionSymbol>> GetOptionSymbolsAsync(string baseAsset, CancellationToken cancellationToken)
    {
        var results = new List<BybitOptionSymbol>();
        string? cursor = null;

        do
        {
            var result = await restClient.V5Api.ExchangeData.GetOptionSymbolsAsync(
                symbol: null,
                baseAsset: baseAsset,
                limit: 1000,
                cursor: cursor,
                ct: cancellationToken);

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
            var result = await restClient.V5Api.ExchangeData.GetLinearInverseSymbolsAsync(
                Category.Linear,
                symbol: null,
                baseAsset: baseAsset,
                status: null,
                symbolType: null,
                limit: 1000,
                cursor: cursor,
                ct: cancellationToken);

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
}
