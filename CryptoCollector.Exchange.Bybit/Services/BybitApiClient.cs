using Bybit.Net.Clients;
using Bybit.Net.Enums;
using Bybit.Net.Objects.Models.V5;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Bybit.Options;
using CryptoExchange.Net.Objects;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text.RegularExpressions;

namespace CryptoCollector.Exchange.Bybit.Services;

public sealed class BybitApiClient(
    BybitRestClient restClient,
    IOptions<BybitCollectorOptions> options,
    ILogger<BybitApiClient> logger)
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

    public async Task<IReadOnlyList<OptionChainSnapshot>> GetOptionChainSnapshotsAsync(CancellationToken cancellationToken)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var tickers = await GetOptionTickersAsync(_options.BaseAsset, cancellationToken);

        return tickers.Select(ticker => new OptionChainSnapshot
        {
            Symbol = ticker.Symbol,
            Ticker = MapOptionTicker(ticker, timestamp)
        }).ToArray();
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

            logger.LogWarning("Bybit REST operation failed. Operation={Operation}, Attempt={Attempt}/{RetryCount}, Error={Error}.",
                operation,
                attempt,
                _options.RestRetryCount,
                lastResult.Error);

            if (attempt == _options.RestRetryCount)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromTicks(_options.RestRetryDelay.Ticks * attempt), cancellationToken);
        }

        logger.LogError("Bybit REST operation exhausted retries. Operation={Operation}, Error={Error}.", operation, lastResult?.Error);
        throw new InvalidOperationException($"{operation} failed: {lastResult?.Error}");
    }

    private static InstrumentDefinition MapOptionInstrument(BybitOptionSymbol source) =>
        new()
        {
            Exchange = "bybit",
            InstrumentType = InstrumentType.Option,
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
            InstrumentType = ResolveDerivativeInstrumentType(source),
            Symbol = source.Name,
            BaseAsset = source.BaseAsset,
            QuoteAsset = source.QuoteAsset,
            SettleAsset = source.SettleAsset,
            ExpiryUtc = source.DeliveryTime,
            StrikePrice = null,
            OptionSide = null
        };

    private static InstrumentType ResolveDerivativeInstrumentType(BybitLinearInverseSymbol source)
    {
        var isPerpetual = source.ContractType is ContractTypeV5.LinearPerpetual or ContractTypeV5.InversePerpetual;
        var isInverse = source.SettleAsset.Equals(source.BaseAsset, StringComparison.OrdinalIgnoreCase) &&
                        !source.QuoteAsset.Equals(source.BaseAsset, StringComparison.OrdinalIgnoreCase);

        if (isPerpetual)
        {
            return isInverse ? InstrumentType.InversePerpetual : InstrumentType.LinearPerpetual;
        }

        return isInverse ? InstrumentType.InverseFutures : InstrumentType.LinearFutures;
    }

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

    public static ExchangeTicker MapLinearTicker(BybitLinearInverseTicker ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            LastPrice = ticker.LastPrice,
            MarkPrice = ticker.MarkPrice,
            IndexPrice = ticker.IndexPrice,
            BidPrice = ticker.BestBidPrice,
            BidSize = ticker.BestBidQuantity,
            AskPrice = ticker.BestAskPrice,
            AskSize = ticker.BestAskQuantity,
            OpenInterest = ticker.OpenInterest,
            OpenInterestValue = ticker.OpenInterestValue,
            Volume24h = ticker.Volume24h,
            Turnover24h = ticker.Turnover24h,
            FundingRate = ticker.FundingRate,
            BasisRate = ticker.BasisRate,
            BasisRateYear = ticker.BasisRateYear,
            DeliveryUtc = ticker.DeliveryTime,
            NextFundingTimeUtc = ticker.NextFundingTime
        };

    public static ExchangeTicker MapLinearTicker(BybitLinearTickerUpdate ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            LastPrice = ticker.LastPrice,
            MarkPrice = ticker.MarkPrice,
            IndexPrice = ticker.IndexPrice,
            BidPrice = ticker.BestBidPrice,
            BidSize = ticker.BestBidQuantity,
            AskPrice = ticker.BestAskPrice,
            AskSize = ticker.BestAskQuantity,
            OpenInterest = ticker.OpenInterest,
            OpenInterestValue = ticker.OpenInterestValue,
            Volume24h = ticker.Volume24h,
            Turnover24h = ticker.Turnover24h,
            FundingRate = ticker.FundingRate,
            BasisRate = ticker.BasisRate,
            BasisRateYear = ticker.BasisRateYear,
            DeliveryUtc = ticker.DeliveryTime,
            NextFundingTimeUtc = ticker.NextFundingTime
        };

    public static ExchangeOptionTicker MapOptionTicker(BybitOptionTicker ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            BidPrice = ticker.BestBidPrice,
            BidSize = ticker.BestBidQuantity,
            BidIv = ticker.BestBidIv,
            AskPrice = ticker.BestAskPrice,
            AskSize = ticker.BestAskQuantity,
            AskIv = ticker.BestAskIv,
            LastPrice = ticker.LastPrice,
            MarkPrice = ticker.MarkPrice,
            IndexPrice = ticker.IndexPrice,
            MarkIv = ticker.MarkIv,
            UnderlyingPrice = ticker.UnderlyingPrice,
            OpenInterest = ticker.OpenInterest,
            Volume24h = ticker.Volume24h,
            Turnover24h = ticker.Turnover24h,
            TotalVolume = ticker.TotalVolume,
            TotalTurnover = ticker.TotalTurnover,
            Delta = ticker.Delta,
            Gamma = ticker.Gamma,
            Vega = ticker.Vega,
            Theta = ticker.Theta,
            Change24h = ticker.Change24h
        };

    public static ExchangeOptionTicker MapOptionTicker(BybitOptionTickerUpdate ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            BidPrice = ticker.BestBidPrice,
            BidSize = ticker.BestBidQuantity,
            BidIv = ticker.BidIv,
            AskPrice = ticker.BestAskPrice,
            AskSize = ticker.BestAskQuantity,
            AskIv = ticker.AskIv,
            LastPrice = ticker.LastPrice,
            MarkPrice = ticker.MarkPrice,
            IndexPrice = ticker.IndexPrice,
            MarkIv = ticker.MarkPriceIv,
            UnderlyingPrice = ticker.UnderlyingPrice,
            OpenInterest = ticker.OpenInterest,
            Volume24h = ticker.Volume24h,
            Turnover24h = ticker.Turnover24h,
            TotalVolume = ticker.TotalVolume,
            TotalTurnover = ticker.TotalTurnover,
            Delta = ticker.Delta,
            Gamma = ticker.Gamma,
            Vega = ticker.Vega,
            Theta = ticker.Theta,
            Change24h = ticker.Change24h
        };
}
