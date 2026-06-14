using Bybit.Net.Clients;
using Bybit.Net.Objects.Options;
using CryptoCollector.Api.Models;
using CryptoCollector.Api.Options;
using CryptoCollector.Api.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<StorageOptions>(builder.Configuration.GetSection(StorageOptions.SectionName));
builder.Services.Configure<BybitCollectorOptions>(builder.Configuration.GetSection(BybitCollectorOptions.SectionName));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddOpenApi();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton(static _ =>
{
    var handler = new SocketsHttpHandler
    {
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate | DecompressionMethods.Brotli,
        ConnectCallback = static async (context, cancellationToken) =>
        {
            var addresses = await Dns.GetHostAddressesAsync(context.DnsEndPoint.Host, cancellationToken);
            var orderedAddresses = addresses
                .OrderByDescending(static x => x.AddressFamily == AddressFamily.InterNetwork)
                .ToArray();

            Exception? lastException = null;

            foreach (var address in orderedAddresses)
            {
                var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                try
                {
                    await socket.ConnectAsync(new IPEndPoint(address, context.DnsEndPoint.Port), cancellationToken);
                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch (Exception exception)
                {
                    lastException = exception;
                    socket.Dispose();
                }
            }

            throw lastException ?? new SocketException((int)SocketError.HostNotFound);
        }
    };

    return new BybitRestClient(
        new HttpClient(handler, disposeHandler: true),
        NullLoggerFactory.Instance,
        Microsoft.Extensions.Options.Options.Create(new BybitRestOptions()));
});
builder.Services.AddSingleton(_ => new BybitSocketClient());
builder.Services.AddSingleton<BybitApiClient>();

builder.Services.AddSingleton<InstrumentCatalog>();
builder.Services.AddSingleton<DailyParquetStore>();
builder.Services.AddSingleton<MinuteAggregationService>();
builder.Services.AddSingleton<IMarketDataSink>(sp => sp.GetRequiredService<MinuteAggregationService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<MinuteAggregationService>());
builder.Services.AddHostedService<BybitCollectorHostedService>();

var app = builder.Build();

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
    string exchange,
    DateTimeOffset? from,
    DateTimeOffset? to,
    string? symbol,
    bool? blockTradesOnly,
    decimal? quantity,
    DailyParquetStore store,
    CancellationToken cancellationToken) =>
{
    if (!exchange.Equals("bybit", StringComparison.OrdinalIgnoreCase))
    {
        return Results.BadRequest("Unsupported exchange.");
    }

    var effectiveTo = to ?? DateTimeOffset.UtcNow;
    var effectiveFrom = from ?? effectiveTo.AddHours(-24);

    if (effectiveFrom > effectiveTo)
    {
        return Results.BadRequest("'from' must be earlier than or equal to 'to'.");
    }

    var rows = await store.QueryAsync<TradeRecord>(
        exchange,
        DataSetNames.Trades,
        effectiveFrom.UtcDateTime,
        effectiveTo.UtcDateTime,
        symbol,
        cancellationToken);

    if (blockTradesOnly == true)
    {
        rows = rows.Where(static x => x.IsBlockTrade).ToArray();
    }

    if (quantity is not null)
    {
        rows = rows.Where(x => x.Quantity >= quantity.Value).ToArray();
    }

    return Results.Ok(rows);
})
.WithName("GetTradeHistory")
.WithSummary("Get raw trade history.")
.WithDescription("Returns persisted trade rows from local Parquet storage. Default period is the last 24 hours when 'from' and 'to' are omitted.")
.WithTags("History");

app.MapGet("/history/tickers", async (
    string exchange,
    DateTimeOffset from,
    DateTimeOffset to,
    string? symbol,
    DailyParquetStore store,
    CancellationToken cancellationToken) =>
{
    if (!exchange.Equals("bybit", StringComparison.OrdinalIgnoreCase))
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
        cancellationToken);

    return Results.Ok(rows);
})
.WithName("GetTickerHistory")
.WithSummary("Get aggregated ticker snapshots by minute.")
.WithDescription("Returns one-minute latest ticker snapshot history for futures and perpetual instruments.")
.WithTags("History");

app.MapGet("/history/option-chain", async (
    string exchange,
    DateTimeOffset from,
    DateTimeOffset to,
    string? symbol,
    DailyParquetStore store,
    CancellationToken cancellationToken) =>
{
    if (!exchange.Equals("bybit", StringComparison.OrdinalIgnoreCase))
    {
        return Results.BadRequest("Unsupported exchange.");
    }

    if (from > to)
    {
        return Results.BadRequest("'from' must be earlier than or equal to 'to'.");
    }

    var rows = await store.QueryAsync<OptionChainMinuteBar>(
        exchange,
        DataSetNames.OptionChain,
        from.UtcDateTime,
        to.UtcDateTime,
        symbol,
        cancellationToken);

    return Results.Ok(rows);
})
.WithName("GetOptionChainHistory")
.WithSummary("Get aggregated option-chain snapshots by minute.")
.WithDescription("Returns one-minute latest option ticker snapshots for option contracts.")
.WithTags("History");

app.Run();
