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
    public InstrumentType InstrumentType { get; set; }
    public string BaseAsset { get; set; } = string.Empty;
    public string QuoteAsset { get; set; } = string.Empty;
    public string SettleAsset { get; set; } = string.Empty;
    public DateTime Date { get; set; }
    public DateTime? ExpiryUtc { get; set; }
    public decimal? StrikePrice { get; set; }
    public string? OptionSide { get; set; }
    public string TradeId { get; set; } = string.Empty;
    public string Side { get; set; } = string.Empty;
    public decimal? Contracts { get; set; }
    public decimal? Amount { get; set; }
    public decimal Price { get; set; }
    public decimal? MarkPrice { get; set; }
    public decimal? IndexPrice { get; set; }
    public decimal? Iv { get; set; }
    public decimal? MarkIv { get; set; }
    public string? TickDirection { get; set; }
    public decimal Quantity { get; set; }
    public decimal Notional { get; set; }
    public bool IsBlockTrade { get; set; }
    public string? BlockTradeId { get; set; }
    public int? BlockTradeLegCount { get; set; }
    public string? ComboId { get; set; }
    public string? ComboTradeId { get; set; }
    public string? BlockRfqId { get; set; }
    public string? Liquidation { get; set; }
    public bool IsRpiTrade { get; set; }
    public string? Sequence { get; set; }
}

public sealed class LegacyTradeRecordV3 : ITimeSeriesRecord
{
    public string Exchange { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public InstrumentType InstrumentType { get; set; }
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
    public decimal? Contracts { get; set; }
    public decimal? Amount { get; set; }
    public decimal Price { get; set; }
    public decimal? MarkPrice { get; set; }
    public decimal? IndexPrice { get; set; }
    public decimal? Iv { get; set; }
    public decimal? MarkIv { get; set; }
    public string? TickDirection { get; set; }
    public decimal Quantity { get; set; }
    public decimal Notional { get; set; }
    public bool IsBlockTrade { get; set; }
    public string? BlockTradeId { get; set; }
    public int? BlockTradeLegCount { get; set; }
    public string? ComboId { get; set; }
    public string? ComboTradeId { get; set; }
    public string? BlockRfqId { get; set; }
    public string? Liquidation { get; set; }
    public bool IsRpiTrade { get; set; }
    public string? Sequence { get; set; }

    public TradeRecord Upgrade() =>
        new()
        {
            Exchange = Exchange,
            Symbol = Symbol,
            InstrumentType = InstrumentType,
            BaseAsset = BaseAsset,
            QuoteAsset = QuoteAsset,
            SettleAsset = SettleAsset,
            Date = Date,
            ExpiryUtc = ExpiryUtc,
            StrikePrice = StrikePrice,
            OptionSide = OptionSide,
            TradeId = TradeId,
            Side = Side,
            Contracts = Contracts,
            Amount = Amount,
            Price = Price,
            MarkPrice = MarkPrice,
            IndexPrice = IndexPrice,
            Iv = Iv,
            MarkIv = MarkIv,
            TickDirection = TickDirection,
            Quantity = Quantity,
            Notional = Notional,
            IsBlockTrade = IsBlockTrade,
            BlockTradeId = BlockTradeId,
            BlockTradeLegCount = BlockTradeLegCount,
            ComboId = ComboId,
            ComboTradeId = ComboTradeId,
            BlockRfqId = BlockRfqId,
            Liquidation = Liquidation,
            IsRpiTrade = IsRpiTrade,
            Sequence = Sequence
        };
}

public sealed class LegacyTradeRecordV2 : ITimeSeriesRecord
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
    public decimal? Contracts { get; set; }
    public decimal? Amount { get; set; }
    public decimal Price { get; set; }
    public decimal? MarkPrice { get; set; }
    public decimal? IndexPrice { get; set; }
    public decimal? Iv { get; set; }
    public decimal? MarkIv { get; set; }
    public string? TickDirection { get; set; }
    public decimal Quantity { get; set; }
    public decimal Notional { get; set; }
    public bool IsBlockTrade { get; set; }
    public string? BlockTradeId { get; set; }
    public int? BlockTradeLegCount { get; set; }
    public string? ComboId { get; set; }
    public string? ComboTradeId { get; set; }
    public string? BlockRfqId { get; set; }
    public string? Liquidation { get; set; }
    public bool IsRpiTrade { get; set; }
    public string? Sequence { get; set; }

    public TradeRecord Upgrade() =>
        new()
        {
            Exchange = Exchange,
            Symbol = Symbol,
            InstrumentType = TradeRecordInstrumentTypeResolver.ResolveInstrumentType(
                Symbol,
                MarketType,
                BaseAsset,
                QuoteAsset,
                SettleAsset,
                ExpiryUtc,
                strikePrice: null,
                optionSide: null),
            BaseAsset = BaseAsset,
            QuoteAsset = QuoteAsset,
            SettleAsset = SettleAsset,
            Date = Date,
            ExpiryUtc = ExpiryUtc,
            StrikePrice = StrikePrice,
            OptionSide = OptionSide,
            TradeId = TradeId,
            Side = Side,
            Contracts = Contracts,
            Amount = Amount,
            Price = Price,
            MarkPrice = MarkPrice,
            IndexPrice = IndexPrice,
            Iv = Iv,
            MarkIv = MarkIv,
            TickDirection = TickDirection,
            Quantity = Quantity,
            Notional = Notional,
            IsBlockTrade = IsBlockTrade,
            BlockTradeId = BlockTradeId,
            BlockTradeLegCount = BlockTradeLegCount,
            ComboId = ComboId,
            ComboTradeId = ComboTradeId,
            BlockRfqId = BlockRfqId,
            Liquidation = Liquidation,
            IsRpiTrade = IsRpiTrade,
            Sequence = Sequence
        };
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
            InstrumentType = TradeRecordInstrumentTypeResolver.ResolveInstrumentType(
                Symbol,
                MarketType,
                BaseAsset,
                QuoteAsset,
                SettleAsset,
                ExpiryUtc,
                strikePrice: null,
                optionSide: null),
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
    public InstrumentType InstrumentType { get; set; }
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

public sealed class LegacyTickerMinuteBarV1 : ITimeSeriesRecord
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

    public TickerMinuteBar Upgrade() =>
        new()
        {
            Exchange = Exchange,
            Symbol = Symbol,
            InstrumentType = TradeRecordInstrumentTypeResolver.ResolveInstrumentType(
                Symbol,
                MarketType,
                BaseAsset,
                QuoteAsset,
                SettleAsset,
                ExpiryUtc,
                strikePrice: null,
                optionSide: null),
            BaseAsset = BaseAsset,
            QuoteAsset = QuoteAsset,
            SettleAsset = SettleAsset,
            Date = Date,
            ExpiryUtc = ExpiryUtc,
            LastPrice = LastPrice,
            MarkPrice = MarkPrice,
            IndexPrice = IndexPrice,
            BidPrice = BidPrice,
            BidSize = BidSize,
            AskPrice = AskPrice,
            AskSize = AskSize,
            OpenInterest = OpenInterest,
            OpenInterestValue = OpenInterestValue,
            Volume24h = Volume24h,
            Turnover24h = Turnover24h,
            FundingRate = FundingRate,
            BasisRate = BasisRate,
            BasisRateYear = BasisRateYear,
            DeliveryUtc = DeliveryUtc,
            NextFundingTimeUtc = NextFundingTimeUtc,
            LastUpdateUtc = LastUpdateUtc
        };
}

public sealed class OptionChainMinuteBar : ITimeSeriesRecord
{
    public string Exchange { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public InstrumentType InstrumentType { get; set; }
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

public sealed class LegacyOptionChainMinuteBarV1 : ITimeSeriesRecord
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

    public OptionChainMinuteBar Upgrade() =>
        new()
        {
            Exchange = Exchange,
            Symbol = Symbol,
            InstrumentType = TradeRecordInstrumentTypeResolver.ResolveInstrumentType(
                Symbol,
                MarketType,
                BaseAsset,
                QuoteAsset,
                SettleAsset,
                ExpiryUtc,
                StrikePrice,
                OptionSide),
            BaseAsset = BaseAsset,
            QuoteAsset = QuoteAsset,
            SettleAsset = SettleAsset,
            Date = Date,
            ExpiryUtc = ExpiryUtc,
            StrikePrice = StrikePrice,
            OptionSide = OptionSide,
            BidPrice = BidPrice,
            BidSize = BidSize,
            BidIv = BidIv,
            AskPrice = AskPrice,
            AskSize = AskSize,
            AskIv = AskIv,
            LastPrice = LastPrice,
            MarkPrice = MarkPrice,
            IndexPrice = IndexPrice,
            MarkIv = MarkIv,
            UnderlyingPrice = UnderlyingPrice,
            OpenInterest = OpenInterest,
            Volume24h = Volume24h,
            Turnover24h = Turnover24h,
            TotalVolume = TotalVolume,
            TotalTurnover = TotalTurnover,
            Delta = Delta,
            Gamma = Gamma,
            Vega = Vega,
            Theta = Theta,
            Change24h = Change24h,
            LastUpdateUtc = LastUpdateUtc
        };
}

internal static class TradeRecordInstrumentTypeResolver
{
    public static InstrumentType ResolveInstrumentType(
        string? symbol,
        string? marketType,
        string baseAsset,
        string quoteAsset,
        string settleAsset,
        DateTime? expiryUtc,
        decimal? strikePrice,
        string? optionSide)
    {
        var normalizedMarketType = marketType?.ToLowerInvariant();
        if (normalizedMarketType == "option")
        {
            return InstrumentType.Option;
        }

        if (!string.IsNullOrWhiteSpace(optionSide) || strikePrice is not null)
        {
            return InstrumentType.Option;
        }

        if (normalizedMarketType is "perpetual")
        {
            return ResolveDerivativeInstrumentType(true, baseAsset, quoteAsset, settleAsset);
        }

        if (normalizedMarketType is "future" or "futures")
        {
            return ResolveDerivativeInstrumentType(false, baseAsset, quoteAsset, settleAsset);
        }

        if (normalizedMarketType == "spot")
        {
            return InstrumentType.Spot;
        }

        if (normalizedMarketType == "margin")
        {
            return InstrumentType.Margin;
        }

        if (normalizedMarketType == "cash")
        {
            return InstrumentType.Cash;
        }

        if (!string.IsNullOrWhiteSpace(symbol))
        {
            if (symbol.Contains("PERPETUAL", StringComparison.OrdinalIgnoreCase) ||
                symbol.Contains("PERP", StringComparison.OrdinalIgnoreCase))
            {
                return ResolveDerivativeInstrumentType(true, baseAsset, quoteAsset, settleAsset);
            }

            if (symbol.Contains("-C", StringComparison.OrdinalIgnoreCase) ||
                symbol.Contains("-P", StringComparison.OrdinalIgnoreCase))
            {
                return InstrumentType.Option;
            }
        }

        if (expiryUtc is not null)
        {
            return ResolveDerivativeInstrumentType(false, baseAsset, quoteAsset, settleAsset);
        }

        if (settleAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase) ||
            quoteAsset.Equals("USD", StringComparison.OrdinalIgnoreCase) ||
            quoteAsset.Equals("USDT", StringComparison.OrdinalIgnoreCase) ||
            quoteAsset.Equals("USDC", StringComparison.OrdinalIgnoreCase))
        {
            return InstrumentType.Spot;
        }

        return InstrumentType.Unknown;
    }

    private static InstrumentType ResolveDerivativeInstrumentType(bool isPerpetual, string baseAsset, string quoteAsset, string settleAsset)
    {
        var isInverse = settleAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase) &&
                        !quoteAsset.Equals(baseAsset, StringComparison.OrdinalIgnoreCase);

        if (isPerpetual)
        {
            return isInverse ? InstrumentType.InversePerpetual : InstrumentType.LinearPerpetual;
        }

        return isInverse ? InstrumentType.InverseFutures : InstrumentType.LinearFutures;
    }
}
