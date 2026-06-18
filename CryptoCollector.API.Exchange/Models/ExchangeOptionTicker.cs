namespace CryptoCollector.API.Exchange.Models;

public sealed class ExchangeOptionTicker
{
    public DateTimeOffset TimestampUtc { get; init; }
    public decimal? BidPrice { get; init; }
    public decimal? BidSize { get; init; }
    public decimal? BidIv { get; init; }
    public decimal? AskPrice { get; init; }
    public decimal? AskSize { get; init; }
    public decimal? AskIv { get; init; }
    public decimal? LastPrice { get; init; }
    public decimal? MarkPrice { get; init; }
    public decimal? IndexPrice { get; init; }
    public decimal? MarkIv { get; init; }
    public decimal? UnderlyingPrice { get; init; }
    public decimal? OpenInterest { get; init; }
    public decimal? Volume24h { get; init; }
    public decimal? Turnover24h { get; init; }
    public decimal? TotalVolume { get; init; }
    public decimal? TotalTurnover { get; init; }
    public decimal? Delta { get; init; }
    public decimal? Gamma { get; init; }
    public decimal? Vega { get; init; }
    public decimal? Theta { get; init; }
    public decimal? Change24h { get; init; }
}
