namespace CryptoCollector.API.Exchange.Models;

public sealed class ExchangeTicker
{
    public DateTimeOffset TimestampUtc { get; init; }
    public decimal? LastPrice { get; init; }
    public decimal? MarkPrice { get; init; }
    public decimal? IndexPrice { get; init; }
    public decimal? BidPrice { get; init; }
    public decimal? BidSize { get; init; }
    public decimal? AskPrice { get; init; }
    public decimal? AskSize { get; init; }
    public decimal? OpenInterest { get; init; }
    public decimal? OpenInterestValue { get; init; }
    public decimal? Volume24h { get; init; }
    public decimal? Turnover24h { get; init; }
    public decimal? FundingRate { get; init; }
    public decimal? BasisRate { get; init; }
    public decimal? BasisRateYear { get; init; }
    public DateTime? DeliveryUtc { get; init; }
    public DateTime? NextFundingTimeUtc { get; init; }
}
