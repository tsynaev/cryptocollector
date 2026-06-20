using Binance.Net.Clients;
using Binance.Net.Enums;
using Binance.Net.Interfaces;
using Binance.Net.Objects.Models.Futures;
using Binance.Net.Objects.Models.Spot;
using CryptoCollector.API.Exchange.Models;
using CryptoCollector.Exchange.Binance.Models;
using CryptoCollector.Exchange.Binance.Options;
using CryptoExchange.Net.Objects;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;

namespace CryptoCollector.Exchange.Binance.Services;

public sealed class BinanceApiClient(
    BinanceRestClient restClient,
    HttpClient optionsHttpClient,
    IOptions<BinanceCollectorOptions> options,
    ILogger<BinanceApiClient> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly BinanceCollectorOptions _options = options.Value;

    public string Exchange => "binance";

    public async Task<IReadOnlyList<InstrumentDefinition>> GetTrackedInstrumentsAsync(string baseAsset, string quoteAsset, CancellationToken cancellationToken)
    {
        var futures = await GetUsdFuturesSymbolsAsync(cancellationToken);
        var optionExchangeInfo = await GetOptionExchangeInfoAsync(cancellationToken);

        var futureInstruments = futures
            .Where(static x => x.Status == SymbolStatus.Trading)
            .Where(x => x.BaseAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase))
            .Where(x => x.QuoteAsset.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
            .Select(MapFuturesInstrument);

        var optionInstruments = optionExchangeInfo.OptionSymbols
            .Where(static x => string.Equals(x.Status, "TRADING", StringComparison.OrdinalIgnoreCase))
            .Where(x => x.Underlying.Equals($"{baseAsset}{quoteAsset}", StringComparison.OrdinalIgnoreCase))
            .Where(x => x.QuoteAsset.Equals(quoteAsset, StringComparison.OrdinalIgnoreCase))
            .Select(MapOptionInstrument);

        return futureInstruments.Concat(optionInstruments).ToArray();
    }

    public async Task<IReadOnlyList<IBinanceRecentTrade>> GetRecentFutureTradesAsync(string symbol, CancellationToken cancellationToken)
    {
        var result = await ExecuteWithRetryAsync(
            ct => restClient.UsdFuturesApi.ExchangeData.GetRecentTradesAsync(
                symbol,
                Math.Clamp(_options.RecentTradesLimit, 1, 500),
                ct),
            $"GetRecentTradesAsync({symbol})",
            cancellationToken);

        return GetData(result, $"GetRecentTradesAsync({symbol})");
    }

    public async Task<IReadOnlyList<ExchangeTickerSnapshot>> GetFuturesTickerSnapshotsAsync(CancellationToken cancellationToken)
    {
        var tickerTask = ExecuteWithRetryAsync(
            ct => restClient.UsdFuturesApi.ExchangeData.GetTickersAsync(ct),
            "GetTickersAsync",
            cancellationToken);
        var markTask = ExecuteWithRetryAsync(
            ct => restClient.UsdFuturesApi.ExchangeData.GetMarkPricesAsync(ct),
            "GetMarkPricesAsync",
            cancellationToken);
        var bookTask = ExecuteWithRetryAsync(
            ct => restClient.UsdFuturesApi.ExchangeData.GetBookPricesAsync(ct),
            "GetBookPricesAsync",
            cancellationToken);

        await Task.WhenAll(tickerTask, markTask, bookTask);

        var tickers = GetData(await tickerTask, "GetTickersAsync").ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var marks = GetData(await markTask, "GetMarkPricesAsync").ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var books = GetData(await bookTask, "GetBookPricesAsync").ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var timestamp = DateTimeOffset.UtcNow;

        return tickers.Keys
            .Concat(marks.Keys)
            .Concat(books.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(symbol => new ExchangeTickerSnapshot(
                symbol,
                MapFuturesTicker(
                    tickers.GetValueOrDefault(symbol),
                    marks.GetValueOrDefault(symbol),
                    books.GetValueOrDefault(symbol),
                    timestamp)))
            .ToArray();
    }

    public async Task<IReadOnlyList<OptionChainSnapshot>> GetOptionChainSnapshotsAsync(CancellationToken cancellationToken)
    {
        var exchangeInfoTask = GetOptionExchangeInfoAsync(cancellationToken);
        var tickerTask = GetOptionTickerStatsAsync(cancellationToken);
        var markTask = GetOptionMarkPricesAsync(cancellationToken);
        var indexTask = GetOptionIndexPriceAsync(cancellationToken);

        await Task.WhenAll(exchangeInfoTask, tickerTask, markTask, indexTask);

        var exchangeInfo = await exchangeInfoTask;
        var tickers = (await tickerTask).ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var marks = (await markTask).ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);
        var indexPrice = await indexTask;
        var timestamp = DateTimeOffset.UtcNow;

        return exchangeInfo.OptionSymbols
            .Where(static x => string.Equals(x.Status, "TRADING", StringComparison.OrdinalIgnoreCase))
            .Where(x => x.Underlying.Equals(_options.UnderlyingSymbol, StringComparison.OrdinalIgnoreCase))
            .Select(symbol => new OptionChainSnapshot
            {
                Symbol = symbol.Symbol,
                Ticker = MapOptionTicker(
                    tickers.GetValueOrDefault(symbol.Symbol),
                    marks.GetValueOrDefault(symbol.Symbol),
                    indexPrice,
                    timestamp)
            })
            .ToArray();
    }

    public async Task<IReadOnlyList<(InstrumentDefinition Instrument, ExchangeTrade Trade)>> GetRecentActiveOptionTradesAsync(
        IReadOnlyList<InstrumentDefinition> instruments,
        DateTime? catchUpFromUtc,
        CancellationToken cancellationToken)
    {
        var trackedOptions = instruments
            .Where(static x => x.InstrumentType == InstrumentType.Option)
            .ToDictionary(static x => x.Symbol, StringComparer.OrdinalIgnoreCase);

        if (trackedOptions.Count == 0)
        {
            return [];
        }

        var tickers = await GetOptionTickerStatsAsync(cancellationToken);
        var activeSymbols = tickers
            .Where(x => trackedOptions.ContainsKey(x.Symbol))
            .Select(x => new
            {
                x.Symbol,
                Volume = ParseDecimalSafe(x.Volume),
                Amount = ParseDecimalSafe(x.Amount)
            })
            .Where(static x => x.Volume > 0 || x.Amount > 0)
            .OrderByDescending(static x => x.Amount)
            .ThenByDescending(static x => x.Volume)
            .Take(Math.Max(0, _options.OptionBootstrapSymbolLimit))
            .Select(static x => x.Symbol)
            .ToArray();

        var result = new List<(InstrumentDefinition Instrument, ExchangeTrade Trade)>();

        foreach (var symbol in activeSymbols)
        {
            var trades = await GetRecentOptionTradesAsync(symbol, cancellationToken);
            if (!trackedOptions.TryGetValue(symbol, out var instrument))
            {
                continue;
            }

            foreach (var trade in trades)
            {
                result.Add((instrument, MapOptionTrade(trade)));
            }
        }

        return result;
    }

    internal Task<BinanceOptionsExchangeInfoResponse> GetOptionExchangeInfoAsync(CancellationToken cancellationToken) =>
        GetOptionsAsync<BinanceOptionsExchangeInfoResponse>("eapi/v1/exchangeInfo", null, "eapi/v1/exchangeInfo", cancellationToken);

    internal Task<IReadOnlyList<BinanceOptionRecentTrade>> GetRecentOptionTradesAsync(string symbol, CancellationToken cancellationToken) =>
        GetOptionsAsync<IReadOnlyList<BinanceOptionRecentTrade>>(
            "eapi/v1/trades",
            new Dictionary<string, string?>
            {
                ["symbol"] = symbol,
                ["limit"] = Math.Min(_options.RecentTradesLimit, 500).ToString(CultureInfo.InvariantCulture)
            },
            $"eapi/v1/trades({symbol})",
            cancellationToken);

    private async Task<IReadOnlyList<BinanceFuturesUsdtSymbol>> GetUsdFuturesSymbolsAsync(CancellationToken cancellationToken)
    {
        var result = await ExecuteWithRetryAsync(
            ct => restClient.UsdFuturesApi.ExchangeData.GetExchangeInfoAsync(ct),
            "GetExchangeInfoAsync",
            cancellationToken);

        return GetData(result, "GetExchangeInfoAsync").Symbols;
    }

    private Task<IReadOnlyList<BinanceOptionTickerStats>> GetOptionTickerStatsAsync(CancellationToken cancellationToken) =>
        GetOptionsAsync<IReadOnlyList<BinanceOptionTickerStats>>("eapi/v1/ticker", null, "eapi/v1/ticker", cancellationToken);

    private Task<IReadOnlyList<BinanceOptionMarkPrice>> GetOptionMarkPricesAsync(CancellationToken cancellationToken) =>
        GetOptionsAsync<IReadOnlyList<BinanceOptionMarkPrice>>("eapi/v1/mark", null, "eapi/v1/mark", cancellationToken);

    private Task<BinanceOptionIndexPrice> GetOptionIndexPriceAsync(CancellationToken cancellationToken) =>
        GetOptionsAsync<BinanceOptionIndexPrice>(
            "eapi/v1/index",
            new Dictionary<string, string?>
            {
                ["underlying"] = _options.UnderlyingSymbol
            },
            "eapi/v1/index",
            cancellationToken);

    private async Task<T> GetOptionsAsync<T>(
        string path,
        IReadOnlyDictionary<string, string?>? query,
        string operation,
        CancellationToken cancellationToken)
        where T : notnull
    {
        return await ExecuteWithRetryAsync(
            async ct =>
            {
                var uri = BuildUri(path, query);
                using var response = await optionsHttpClient.GetAsync(uri, ct);
                if (!response.IsSuccessStatusCode)
                {
                    var body = await response.Content.ReadAsStringAsync(ct);
                    logger.LogError("Binance options REST request failed. Operation={Operation}, StatusCode={StatusCode}, Response={Response}.",
                        operation,
                        (int)response.StatusCode,
                        body);
                    response.EnsureSuccessStatusCode();
                }

                var payload = await response.Content.ReadFromJsonAsync<T>(JsonOptions, ct);
                if (payload is null)
                {
                    throw new InvalidOperationException($"{operation} returned no payload.");
                }

                return payload;
            },
            operation,
            cancellationToken);
    }

    private static Uri BuildUri(string path, IReadOnlyDictionary<string, string?>? query)
    {
        if (query is null || query.Count == 0)
        {
            return new Uri(path, UriKind.Relative);
        }

        var queryString = string.Join("&",
            query.Where(static x => !string.IsNullOrWhiteSpace(x.Value))
                 .Select(x => $"{Uri.EscapeDataString(x.Key)}={Uri.EscapeDataString(x.Value!)}"));

        return new Uri($"{path}?{queryString}", UriKind.Relative);
    }

    private static T GetData<T>(WebCallResult<T> result, string operation)
        where T : class
    {
        if (!result.Success || result.Data is null)
        {
            throw new InvalidOperationException($"{operation} failed: {result.Error}");
        }

        return result.Data;
    }

    private async Task<WebCallResult<T>> ExecuteWithRetryAsync<T>(
        Func<CancellationToken, Task<WebCallResult<T>>> action,
        string operation,
        CancellationToken cancellationToken)
        where T : class
    {
        WebCallResult<T>? lastResult = null;

        for (var attempt = 1; attempt <= _options.RestRetryCount; attempt++)
        {
            lastResult = await action(cancellationToken);
            if (lastResult.Success && lastResult.Data is not null)
            {
                return lastResult;
            }

            logger.LogWarning("Binance REST operation failed. Operation={Operation}, Attempt={Attempt}/{RetryCount}, Error={Error}.",
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

        throw new InvalidOperationException($"{operation} failed: {lastResult?.Error}");
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
                logger.LogWarning(exception, "Binance REST operation failed. Operation={Operation}, Attempt={Attempt}/{RetryCount}.",
                    operation,
                    attempt,
                    _options.RestRetryCount);
                await Task.Delay(TimeSpan.FromTicks(_options.RestRetryDelay.Ticks * attempt), cancellationToken);
            }
        }

        throw new InvalidOperationException($"{operation} failed.", lastException);
    }

    private static InstrumentDefinition MapFuturesInstrument(BinanceFuturesUsdtSymbol source) =>
        new()
        {
            Exchange = "binance",
            InstrumentType = source.ContractType == ContractType.Perpetual
                ? InstrumentType.LinearPerpetual
                : InstrumentType.LinearFutures,
            Symbol = source.Name,
            BaseAsset = source.BaseAsset,
            QuoteAsset = source.QuoteAsset,
            SettleAsset = source.MarginAsset,
            ExpiryUtc = source.ContractType == ContractType.Perpetual ? null : source.DeliveryDate,
            StrikePrice = null,
            OptionSide = null
        };

    private static InstrumentDefinition MapOptionInstrument(BinanceOptionSymbol source) =>
        new()
        {
            Exchange = "binance",
            InstrumentType = InstrumentType.Option,
            Symbol = source.Symbol,
            BaseAsset = GetBaseAssetFromUnderlying(source.Underlying),
            QuoteAsset = source.QuoteAsset,
            SettleAsset = source.QuoteAsset,
            ExpiryUtc = DateTimeOffset.FromUnixTimeMilliseconds(source.ExpiryDate).UtcDateTime,
            StrikePrice = ParseDecimal(source.StrikePrice),
            OptionSide = string.Equals(source.Side, "CALL", StringComparison.OrdinalIgnoreCase) ? "Call" : "Put"
        };

    public static ExchangeTrade MapFutureTrade(string symbol, IBinanceRecentTrade trade) =>
        new()
        {
            TradeTime = new DateTimeOffset(trade.TradeTime),
            Symbol = symbol,
            Side = trade.BuyerIsMaker ? "Sell" : "Buy",
            Quantity = trade.BaseQuantity,
            Contracts = null,
            Amount = trade.QuoteQuantity,
            Price = trade.Price,
            MarkPrice = null,
            IndexPrice = null,
            Iv = null,
            MarkIv = null,
            TickDirection = null,
            TradeId = trade.OrderId.ToString(CultureInfo.InvariantCulture),
            IsBlockTrade = false,
            BlockTradeId = null,
            BlockTradeLegCount = null,
            ComboId = null,
            ComboTradeId = null,
            BlockRfqId = null,
            Liquidation = null,
            IsRpiTrade = trade.IsRpiTrade ?? false,
            Sequence = trade.OrderId.ToString(CultureInfo.InvariantCulture)
        };

    public static ExchangeTrade MapFutureTrade(global::Binance.Net.Objects.Models.Spot.Socket.BinanceStreamTrade trade) =>
        new()
        {
            TradeTime = new DateTimeOffset(trade.TradeTime),
            Symbol = trade.Symbol,
            Side = trade.BuyerIsMaker ? "Sell" : "Buy",
            Quantity = trade.Quantity,
            Contracts = null,
            Amount = trade.Price * trade.Quantity,
            Price = trade.Price,
            MarkPrice = null,
            IndexPrice = null,
            Iv = null,
            MarkIv = null,
            TickDirection = trade.Type,
            TradeId = trade.Id.ToString(CultureInfo.InvariantCulture),
            IsBlockTrade = false,
            BlockTradeId = null,
            BlockTradeLegCount = null,
            ComboId = null,
            ComboTradeId = null,
            BlockRfqId = null,
            Liquidation = null,
            IsRpiTrade = false,
            Sequence = trade.Id.ToString(CultureInfo.InvariantCulture)
        };

    internal static ExchangeTrade MapOptionTrade(BinanceOptionRecentTrade trade) =>
        new()
        {
            TradeTime = DateTimeOffset.FromUnixTimeMilliseconds(trade.Time),
            Symbol = trade.Symbol,
            Side = trade.Side >= 0 ? "Buy" : "Sell",
            Quantity = ParseDecimal(trade.Qty),
            Contracts = null,
            Amount = ParseDecimal(trade.QuoteQty),
            Price = ParseDecimal(trade.Price),
            MarkPrice = null,
            IndexPrice = null,
            Iv = null,
            MarkIv = null,
            TickDirection = null,
            TradeId = trade.TradeId.ToString(CultureInfo.InvariantCulture),
            IsBlockTrade = false,
            BlockTradeId = null,
            BlockTradeLegCount = null,
            ComboId = null,
            ComboTradeId = null,
            BlockRfqId = null,
            Liquidation = null,
            IsRpiTrade = false,
            Sequence = trade.Id.ToString(CultureInfo.InvariantCulture)
        };

    internal static ExchangeTrade MapOptionTrade(BinanceOptionTradeStreamMessage trade) =>
        new()
        {
            TradeTime = DateTimeOffset.FromUnixTimeMilliseconds(trade.TradeTime),
            Symbol = trade.Symbol,
            Side = trade.Side ?? (trade.BuyerIsMarketMaker ? "Sell" : "Buy"),
            Quantity = ParseDecimal(trade.Quantity),
            Contracts = null,
            Amount = ParseDecimal(trade.Price) * ParseDecimal(trade.Quantity),
            Price = ParseDecimal(trade.Price),
            MarkPrice = null,
            IndexPrice = null,
            Iv = null,
            MarkIv = null,
            TickDirection = trade.TradeType,
            TradeId = trade.TradeId.ToString(CultureInfo.InvariantCulture),
            IsBlockTrade = string.Equals(trade.TradeType, "BLOCK", StringComparison.OrdinalIgnoreCase),
            BlockTradeId = null,
            BlockTradeLegCount = null,
            ComboId = null,
            ComboTradeId = null,
            BlockRfqId = null,
            Liquidation = null,
            IsRpiTrade = false,
            Sequence = trade.TradeId.ToString(CultureInfo.InvariantCulture)
        };

    public static ExchangeTicker MapFuturesTicker(
        IBinance24HPrice? ticker,
        BinanceFuturesMarkPrice? mark,
        IBinanceBookPrice? book,
        DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            LastPrice = ticker?.LastPrice,
            MarkPrice = mark?.MarkPrice,
            IndexPrice = mark?.IndexPrice,
            BidPrice = book?.BestBidPrice,
            BidSize = book?.BestBidQuantity,
            AskPrice = book?.BestAskPrice,
            AskSize = book?.BestAskQuantity,
            OpenInterest = null,
            OpenInterestValue = null,
            Volume24h = ticker?.Volume,
            Turnover24h = ticker?.QuoteVolume,
            FundingRate = mark?.FundingRate,
            BasisRate = null,
            BasisRateYear = null,
            DeliveryUtc = null,
            NextFundingTimeUtc = mark?.NextFundingTime
        };

    public static ExchangeTicker MapFuturesTicker(IBinance24HPrice ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            LastPrice = ticker.LastPrice,
            Volume24h = ticker.Volume,
            Turnover24h = ticker.QuoteVolume
        };

    public static ExchangeTicker MapFuturesTicker(BinanceFuturesMarkPrice ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            MarkPrice = ticker.MarkPrice,
            IndexPrice = ticker.IndexPrice,
            FundingRate = ticker.FundingRate,
            NextFundingTimeUtc = ticker.NextFundingTime
        };

    public static ExchangeTicker MapFuturesTicker(global::Binance.Net.Objects.Models.Futures.Socket.BinanceFuturesUsdtStreamMarkPrice ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            MarkPrice = ticker.MarkPrice,
            IndexPrice = ticker.IndexPrice,
            FundingRate = ticker.FundingRate,
            NextFundingTimeUtc = ticker.NextFundingTime
        };

    public static ExchangeTicker MapFuturesTicker(global::Binance.Net.Objects.Models.Futures.Socket.BinanceFuturesStreamBookPrice ticker, DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            BidPrice = ticker.BestBidPrice,
            BidSize = ticker.BestBidQuantity,
            AskPrice = ticker.BestAskPrice,
            AskSize = ticker.BestAskQuantity
        };

    private static ExchangeOptionTicker MapOptionTicker(
        BinanceOptionTickerStats? ticker,
        BinanceOptionMarkPrice? mark,
        BinanceOptionIndexPrice indexPrice,
        DateTimeOffset timestampUtc) =>
        new()
        {
            TimestampUtc = timestampUtc,
            BidPrice = ticker is null ? null : ParseDecimal(ticker.BidPrice),
            BidSize = null,
            BidIv = mark is null ? null : ParseDecimal(mark.BidIV),
            AskPrice = ticker is null ? null : ParseDecimal(ticker.AskPrice),
            AskSize = null,
            AskIv = mark is null ? null : ParseDecimal(mark.AskIV),
            LastPrice = ticker is null ? null : ParseDecimal(ticker.LastPrice),
            MarkPrice = mark is null ? null : ParseDecimal(mark.MarkPrice),
            IndexPrice = ParseDecimal(indexPrice.IndexPrice),
            MarkIv = mark is null ? null : ParseDecimal(mark.MarkIV),
            UnderlyingPrice = ParseDecimal(indexPrice.IndexPrice),
            OpenInterest = null,
            Volume24h = ticker is null ? null : ParseDecimal(ticker.Volume),
            Turnover24h = ticker is null ? null : ParseDecimal(ticker.Amount),
            TotalVolume = ticker is null ? null : ParseDecimal(ticker.Volume),
            TotalTurnover = ticker is null ? null : ParseDecimal(ticker.Amount),
            Delta = mark is null ? null : ParseDecimal(mark.Delta),
            Gamma = mark is null ? null : ParseDecimal(mark.Gamma),
            Vega = mark is null ? null : ParseDecimal(mark.Vega),
            Theta = mark is null ? null : ParseDecimal(mark.Theta),
            Change24h = ticker is null ? null : ParseDecimal(ticker.PriceChangePercent)
        };

    private static string GetBaseAssetFromUnderlying(string underlying)
    {
        if (underlying.EndsWith("USDT", StringComparison.OrdinalIgnoreCase))
        {
            return underlying[..^4];
        }

        if (underlying.EndsWith("USDC", StringComparison.OrdinalIgnoreCase))
        {
            return underlying[..^4];
        }

        if (underlying.EndsWith("USD", StringComparison.OrdinalIgnoreCase))
        {
            return underlying[..^3];
        }

        return underlying;
    }

    private static decimal ParseDecimal(string value) =>
        decimal.Parse(value, NumberStyles.Any, CultureInfo.InvariantCulture);

    private static decimal ParseDecimalSafe(string? value) =>
        decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : 0m;

    public sealed record ExchangeTickerSnapshot(string Symbol, ExchangeTicker Ticker);
}
