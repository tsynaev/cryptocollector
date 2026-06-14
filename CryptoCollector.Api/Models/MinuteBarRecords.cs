using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.Api.Models;

public interface ITimeSeriesRecord
{
    string Exchange { get; }
    string Symbol { get; }
    DateTime Date { get; }
}

public sealed class TradeRecord : ITimeSeriesRecord
{
    public string Exchange { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public string MarketType { get; set; } = string.Empty;
    public string BaseAsset { get; set; } = string.Empty;
    public string QuoteAsset { get; set; } = string.Empty;
    public string SettleAsset { get; set; } = string.Empty;
    public DateTime Date { get; set; }
    public DateTime? ExpiryUtc { get; set; }
    public decimal? StrikePrice { get; set; }
    public string? OptionSide { get; set; }
    public string TradeId { get; set; } = string.Empty;
    public string Side { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public decimal Quantity { get; set; }
    public decimal Notional { get; set; }
    public bool IsBlockTrade { get; set; }
    public string? BlockTradeId { get; set; }
    public bool IsRpiTrade { get; set; }
    public string? Sequence { get; set; }
}

public sealed class LegacyTradeRecordV1 : ITimeSeriesRecord
{
    public string Exchange { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public string MarketType { get; set; } = string.Empty;
    public string BaseAsset { get; set; } = string.Empty;
    public string QuoteAsset { get; set; } = string.Empty;
    public string SettleAsset { get; set; } = string.Empty;
    public DateTime Date { get; set; }
    public DateTime? ExpiryUtc { get; set; }
    public decimal? StrikePrice { get; set; }
    public string? OptionSide { get; set; }
    public string TradeId { get; set; } = string.Empty;
    public string Side { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public decimal Quantity { get; set; }
    public decimal Notional { get; set; }
    public bool IsBlockTrade { get; set; }
    public bool IsRpiTrade { get; set; }
    public string? Sequence { get; set; }

    public TradeRecord Upgrade() =>
        new()
        {
            Exchange = Exchange,
            Symbol = Symbol,
            MarketType = MarketType,
            BaseAsset = BaseAsset,
            QuoteAsset = QuoteAsset,
            SettleAsset = SettleAsset,
            Date = Date,
            ExpiryUtc = ExpiryUtc,
            StrikePrice = StrikePrice,
            OptionSide = OptionSide,
            TradeId = TradeId,
            Side = Side,
            Price = Price,
            Quantity = Quantity,
            Notional = Notional,
            IsBlockTrade = IsBlockTrade,
            BlockTradeId = null,
            IsRpiTrade = IsRpiTrade,
            Sequence = Sequence
        };
}

public sealed class TickerMinuteBar : ITimeSeriesRecord
{
    public string Exchange { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public string MarketType { get; set; } = string.Empty;
    public string BaseAsset { get; set; } = string.Empty;
    public string QuoteAsset { get; set; } = string.Empty;
    public string SettleAsset { get; set; } = string.Empty;
    public DateTime Date { get; set; }
    public DateTime? ExpiryUtc { get; set; }
    public decimal? LastPrice { get; set; }
    public decimal? MarkPrice { get; set; }
    public decimal? IndexPrice { get; set; }
    public decimal? BidPrice { get; set; }
    public decimal? BidSize { get; set; }
    public decimal? AskPrice { get; set; }
    public decimal? AskSize { get; set; }
    public decimal? OpenInterest { get; set; }
    public decimal? OpenInterestValue { get; set; }
    public decimal? Volume24h { get; set; }
    public decimal? Turnover24h { get; set; }
    public decimal? FundingRate { get; set; }
    public decimal? BasisRate { get; set; }
    public decimal? BasisRateYear { get; set; }
    public DateTime? DeliveryUtc { get; set; }
    public DateTime? NextFundingTimeUtc { get; set; }
    public DateTime LastUpdateUtc { get; set; }
}

public sealed class OptionChainMinuteBar : ITimeSeriesRecord
{
    public string Exchange { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public string MarketType { get; set; } = string.Empty;
    public string BaseAsset { get; set; } = string.Empty;
    public string QuoteAsset { get; set; } = string.Empty;
    public string SettleAsset { get; set; } = string.Empty;
    public DateTime Date { get; set; }
    public DateTime? ExpiryUtc { get; set; }
    public decimal? StrikePrice { get; set; }
    public string? OptionSide { get; set; }
    public decimal? BidPrice { get; set; }
    public decimal? BidSize { get; set; }
    public decimal? BidIv { get; set; }
    public decimal? AskPrice { get; set; }
    public decimal? AskSize { get; set; }
    public decimal? AskIv { get; set; }
    public decimal? LastPrice { get; set; }
    public decimal? MarkPrice { get; set; }
    public decimal? IndexPrice { get; set; }
    public decimal? MarkIv { get; set; }
    public decimal? UnderlyingPrice { get; set; }
    public decimal? OpenInterest { get; set; }
    public decimal? Volume24h { get; set; }
    public decimal? Turnover24h { get; set; }
    public decimal? TotalVolume { get; set; }
    public decimal? TotalTurnover { get; set; }
    public decimal? Delta { get; set; }
    public decimal? Gamma { get; set; }
    public decimal? Vega { get; set; }
    public decimal? Theta { get; set; }
    public decimal? Change24h { get; set; }
    public DateTime LastUpdateUtc { get; set; }
}
