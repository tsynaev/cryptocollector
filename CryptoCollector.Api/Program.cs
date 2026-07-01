using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;
using CryptoCollector.Api.Models;
using CryptoCollector.Api.Options;
using CryptoCollector.Api.Services;
using CryptoCollector.Exchange.Binance.Services;
using CryptoCollector.Exchange.Bybit.Services;
using CryptoCollector.Exchange.Deribit.Services;
using CryptoCollector.Exchange.Binance;
using CryptoCollector.Exchange.Bybit;
using CryptoCollector.Exchange.Deribit;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<StorageOptions>(builder.Configuration.GetSection(StorageOptions.SectionName));
builder.Services.Configure<AggregationOptions>(builder.Configuration.GetSection(AggregationOptions.SectionName));
builder.Services.Configure<BlockTradesAlertOptions>(builder.Configuration.GetSection(BlockTradesAlertOptions.SectionName));
builder.Services
    .AddOptions<TelegramOptions>()
    .Bind(builder.Configuration.GetSection(TelegramOptions.SectionName))
    .Validate(
        options => !options.Enabled || (!string.IsNullOrWhiteSpace(options.BotToken) && !string.IsNullOrWhiteSpace(options.ChatId)),
        "Telegram:BotToken and Telegram:ChatId are required when Telegram:Enabled=true.")
    .ValidateOnStart();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddOpenApi();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<DailyParquetStore>();
builder.Services.AddSingleton<BlackScholesPricer>();
builder.Services.AddSingleton<PositionPnlChartRenderer>();
builder.Services.AddSingleton<ILocalMessageBus, LocalMessageBus>();
builder.Services.AddHttpClient(nameof(TelegramMessageQueue));
builder.Services.AddSingleton<TelegramMessageQueue>();
builder.Services.AddSingleton<IMessageQueue>(sp => sp.GetRequiredService<TelegramMessageQueue>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<TelegramMessageQueue>());
builder.Services.AddSingleton<BlockTradeAlertService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<BlockTradeAlertService>());
builder.Services.AddSingleton<MinuteAggregationService>();
builder.Services.AddSingleton<IMarketDataSink, CompositeMarketDataSink>();
builder.Services.AddSingleton<IFlushableMarketDataSink>(sp => (CompositeMarketDataSink)sp.GetRequiredService<IMarketDataSink>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<MinuteAggregationService>());
builder.Services.AddBinanceExchange(builder.Configuration);
builder.Services.AddBybitExchange(builder.Configuration);
builder.Services.AddDeribitExchange(builder.Configuration);
builder.Services.AddSingleton<IHostedService>(sp => new ExchangeCollectorService(
    sp.GetRequiredService<BinanceExchange>(),
    sp.GetRequiredService<DailyParquetStore>(),
    sp.GetRequiredService<IMarketDataSink>(),
    sp.GetRequiredService<IFlushableMarketDataSink>(),
    sp.GetRequiredService<ILogger<ExchangeCollectorService>>()));
builder.Services.AddSingleton<IHostedService>(sp => new ExchangeCollectorService(
    sp.GetRequiredService<BybitExchange>(),
    sp.GetRequiredService<DailyParquetStore>(),
    sp.GetRequiredService<IMarketDataSink>(),
    sp.GetRequiredService<IFlushableMarketDataSink>(),
    sp.GetRequiredService<ILogger<ExchangeCollectorService>>()));
builder.Services.AddSingleton<IHostedService>(sp => new ExchangeCollectorService(
    sp.GetRequiredService<DeribitExchange>(),
    sp.GetRequiredService<DailyParquetStore>(),
    sp.GetRequiredService<IMarketDataSink>(),
    sp.GetRequiredService<IFlushableMarketDataSink>(),
    sp.GetRequiredService<ILogger<ExchangeCollectorService>>()));

var app = builder.Build();
var SupportedExchanges = new[] { "binance", "bybit", "deribit" };

app.UseSwagger();
app.UseSwaggerUI();
app.MapOpenApi();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    utcNow = DateTimeOffset.UtcNow
}))
.WithName("Health")
.WithTags("System");

app.MapGet("/history/trades", async (
    string? exchange,
    DateTimeOffset? from,
    DateTimeOffset? to,
    string? symbol,
    InstrumentType? instrumentType,
    bool? blockTradesOnly,
    decimal? quantity,
    DailyParquetStore store,
    CancellationToken cancellationToken) =>
{
    if (!string.IsNullOrWhiteSpace(exchange) && !IsSupportedExchange(exchange))
    {
        return Results.BadRequest("Unsupported exchange.");
    }

    var effectiveTo = to ?? DateTimeOffset.UtcNow;
    var effectiveFrom = from ?? effectiveTo.AddHours(-24);

    if (effectiveFrom > effectiveTo)
    {
        return Results.BadRequest("'from' must be earlier than or equal to 'to'.");
    }

    var exchanges = string.IsNullOrWhiteSpace(exchange)
        ? SupportedExchanges
        : [exchange];

    bool Predicate(TradeRecord row)
    {
        if (instrumentType is not null && row.InstrumentType != instrumentType.Value)
        {
            return false;
        }

        if (blockTradesOnly == true && !row.IsBlockTrade)
        {
            return false;
        }

        if (quantity is not null && row.Quantity < quantity.Value)
        {
            return false;
        }

        return true;
    }

    var rows = await store.QueryAcrossExchangesAsync<TradeRecord>(
        exchanges,
        DataSetNames.Trades,
        effectiveFrom.UtcDateTime,
        effectiveTo.UtcDateTime,
        symbol,
        Predicate,
        cancellationToken);

    return Results.Ok(rows);
})
.WithName("GetTradeHistory")
.WithSummary("Get raw trade history.")
.WithDescription("Returns persisted trade rows from local Parquet storage. Default period is the last 24 hours when 'from' and 'to' are omitted. When 'exchange' is omitted, rows are returned across all supported exchanges.")
.WithTags("History");

app.MapGet("/history/block-trades", async (
    string? exchange,
    DateTimeOffset? from,
    DateTimeOffset? to,
    decimal? totalQuantity,
    decimal? minGroupUsd,
    DailyParquetStore store,
    CancellationToken cancellationToken) =>
{
    if (!string.IsNullOrWhiteSpace(exchange) && !IsSupportedExchange(exchange))
    {
        return Results.BadRequest("Unsupported exchange.");
    }

    var effectiveTo = to ?? DateTimeOffset.UtcNow;
    var effectiveFrom = from ?? effectiveTo.AddHours(-24);

    if (effectiveFrom > effectiveTo)
    {
        return Results.BadRequest("'from' must be earlier than or equal to 'to'.");
    }

    var exchanges = string.IsNullOrWhiteSpace(exchange)
        ? SupportedExchanges
        : [exchange];

    var trades = await store.QueryAcrossExchangesAsync<EnrichedBlockTradeRecord>(
        exchanges,
        DataSetNames.BlockTrades,
        effectiveFrom.UtcDateTime,
        effectiveTo.UtcDateTime,
        symbol: null,
        predicate: null,
        cancellationToken);

    var groups = BlockTradeHistoryBuilder.Build(trades, minGroupUsd ?? 0m);

    if (totalQuantity is not null)
    {
        groups = groups.Where(x => x.TotalQuantity >= totalQuantity.Value).ToArray();
    }

    if (minGroupUsd is not null)
    {
        groups = groups.Where(x => x.TotalUsdNotional >= minGroupUsd.Value).ToArray();
    }

    return Results.Ok(groups);
})
.WithName("GetBlockTradeHistory")
.WithSummary("Get grouped block trade history with legs.")
.WithDescription("Returns persisted trade history grouped into structured block-trade groups with individual legs included.")
.WithTags("History");

app.MapGet("/history/tickers", async (
    string exchange,
    DateTimeOffset from,
    DateTimeOffset to,
    string? symbol,
    DailyParquetStore store,
    CancellationToken cancellationToken) =>
{
    if (!IsSupportedExchange(exchange))
    {
        return Results.BadRequest("Unsupported exchange.");
    }

    if (from > to)
    {
        return Results.BadRequest("'from' must be earlier than or equal to 'to'.");
    }

    var rows = await store.QueryAsync<TickerMinuteBar>(
        exchange,
        DataSetNames.Tickers,
        from.UtcDateTime,
        to.UtcDateTime,
        symbol,
        predicate: null,
        cancellationToken);

    return Results.Ok(rows);
})
.WithName("GetTickerHistory")
.WithSummary("Get aggregated ticker snapshots by minute.")
.WithDescription("Returns one-minute latest ticker snapshot history for futures and perpetual instruments.")
.WithTags("History");

app.MapGet("/history/option-chain", async (
    string exchange,
    DateTimeOffset? from,
    DateTimeOffset? to,
    string? symbol,
    decimal? strike,
    DateOnly? expireDate,
    string? optionSide,
    DailyParquetStore store,
    CancellationToken cancellationToken) =>
{
    if (!IsSupportedExchange(exchange))
    {
        return Results.BadRequest("Unsupported exchange.");
    }

    if (from is not null && to is not null && from > to)
    {
        return Results.BadRequest("'from' must be earlier than or equal to 'to'.");
    }

    if (!string.IsNullOrWhiteSpace(optionSide) &&
        !optionSide.Equals("call", StringComparison.OrdinalIgnoreCase) &&
        !optionSide.Equals("put", StringComparison.OrdinalIgnoreCase))
    {
        return Results.BadRequest("'optionSide' must be 'call' or 'put'.");
    }

    bool Predicate(OptionChainMinuteBar row)
    {
        if (strike is not null && row.StrikePrice != strike.Value)
        {
            return false;
        }

        if (expireDate is not null)
        {
            var rowExpiryDate = row.ExpiryUtc is null ? (DateOnly?)null : DateOnly.FromDateTime(row.ExpiryUtc.Value);
            if (rowExpiryDate != expireDate.Value)
            {
                return false;
            }
        }

        if (!string.IsNullOrWhiteSpace(optionSide) &&
            !string.Equals(row.OptionSide, optionSide, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return true;
    }

    IReadOnlyList<OptionChainMinuteBar> rows;

    if (from is null && to is null)
    {
        rows = await store.QueryLatestAsync<OptionChainMinuteBar>(
            exchange,
            DataSetNames.OptionChain,
            symbol,
            Predicate,
            cancellationToken);
    }
    else
    {
        var effectiveTo = to ?? DateTimeOffset.UtcNow;
        var effectiveFrom = from ?? effectiveTo.AddHours(-24);

        if (effectiveFrom > effectiveTo)
        {
            return Results.BadRequest("'from' must be earlier than or equal to 'to'.");
        }

        rows = await store.QueryAsync<OptionChainMinuteBar>(
            exchange,
            DataSetNames.OptionChain,
            effectiveFrom.UtcDateTime,
            effectiveTo.UtcDateTime,
            symbol,
            predicate: null,
            cancellationToken);

        rows = rows.Where(Predicate).ToArray();
    }

    return Results.Ok(rows);
})
.WithName("GetOptionChainHistory")
.WithSummary("Get aggregated option-chain snapshots by minute.")
.WithDescription("Returns one-minute option-chain snapshots. When both 'from' and 'to' are omitted, returns the latest available chain snapshot.")
.WithTags("History");

bool IsSupportedExchange(string exchange) =>
    SupportedExchanges.Contains(exchange, StringComparer.OrdinalIgnoreCase);

app.Run();
