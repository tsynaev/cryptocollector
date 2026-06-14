using System.Text.Json.Serialization;

namespace CryptoCollector.Api.Models;

public sealed class BybitEnvelope<T>
{
    [JsonPropertyName("retCode")]
    public int RetCode { get; init; }

    [JsonPropertyName("retMsg")]
    public string RetMsg { get; init; } = string.Empty;

    [JsonPropertyName("result")]
    public T Result { get; init; } = default!;
}

public sealed class BybitInstrumentsResponse
{
    [JsonPropertyName("category")]
    public string Category { get; init; } = string.Empty;

    [JsonPropertyName("nextPageCursor")]
    public string? NextPageCursor { get; init; }

    [JsonPropertyName("list")]
    public IReadOnlyList<BybitInstrumentInfo> List { get; init; } = [];
}

public sealed class BybitInstrumentInfo
{
    [JsonPropertyName("symbol")]
    public string Symbol { get; init; } = string.Empty;

    [JsonPropertyName("status")]
    public string Status { get; init; } = string.Empty;

    [JsonPropertyName("baseCoin")]
    public string BaseCoin { get; init; } = string.Empty;

    [JsonPropertyName("quoteCoin")]
    public string? QuoteCoin { get; init; }

    [JsonPropertyName("settleCoin")]
    public string? SettleCoin { get; init; }

    [JsonPropertyName("category")]
    public string Category { get; init; } = string.Empty;

    [JsonPropertyName("contractType")]
    public string ContractType { get; init; } = string.Empty;

    [JsonPropertyName("deliveryTime")]
    public string? DeliveryTime { get; init; }
}

public sealed class BybitTickersResponse
{
    [JsonPropertyName("category")]
    public string Category { get; init; } = string.Empty;

    [JsonPropertyName("list")]
    public IReadOnlyList<BybitTickerSnapshot> List { get; init; } = [];
}

public sealed class BybitTickerSnapshot
{
    [JsonPropertyName("symbol")]
    public string Symbol { get; init; } = string.Empty;

    [JsonPropertyName("lastPrice")]
    public string? LastPrice { get; init; }

    [JsonPropertyName("markPrice")]
    public string? MarkPrice { get; init; }

    [JsonPropertyName("indexPrice")]
    public string? IndexPrice { get; init; }

    [JsonPropertyName("bid1Price")]
    public string? Bid1Price { get; init; }

    [JsonPropertyName("bid1Size")]
    public string? Bid1Size { get; init; }

    [JsonPropertyName("bid1Iv")]
    public string? Bid1Iv { get; init; }

    [JsonPropertyName("ask1Price")]
    public string? Ask1Price { get; init; }

    [JsonPropertyName("ask1Size")]
    public string? Ask1Size { get; init; }

    [JsonPropertyName("ask1Iv")]
    public string? Ask1Iv { get; init; }

    [JsonPropertyName("openInterest")]
    public string? OpenInterest { get; init; }

    [JsonPropertyName("openInterestValue")]
    public string? OpenInterestValue { get; init; }

    [JsonPropertyName("turnover24h")]
    public string? Turnover24h { get; init; }

    [JsonPropertyName("volume24h")]
    public string? Volume24h { get; init; }

    [JsonPropertyName("fundingRate")]
    public string? FundingRate { get; init; }

    [JsonPropertyName("basisRate")]
    public string? BasisRate { get; init; }

    [JsonPropertyName("basisRateYear")]
    public string? BasisRateYear { get; init; }

    [JsonPropertyName("deliveryTime")]
    public string? DeliveryTime { get; init; }

    [JsonPropertyName("nextFundingTime")]
    public string? NextFundingTime { get; init; }

    [JsonPropertyName("markIv")]
    public string? MarkIv { get; init; }

    [JsonPropertyName("underlyingPrice")]
    public string? UnderlyingPrice { get; init; }

    [JsonPropertyName("totalVolume")]
    public string? TotalVolume { get; init; }

    [JsonPropertyName("totalTurnover")]
    public string? TotalTurnover { get; init; }

    [JsonPropertyName("delta")]
    public string? Delta { get; init; }

    [JsonPropertyName("gamma")]
    public string? Gamma { get; init; }

    [JsonPropertyName("vega")]
    public string? Vega { get; init; }

    [JsonPropertyName("theta")]
    public string? Theta { get; init; }

    [JsonPropertyName("change24h")]
    public string? Change24h { get; init; }
}

public sealed class BybitRecentTradesResponse
{
    [JsonPropertyName("category")]
    public string Category { get; init; } = string.Empty;

    [JsonPropertyName("list")]
    public IReadOnlyList<BybitRecentTrade> List { get; init; } = [];
}

public sealed class BybitRecentTrade
{
    [JsonPropertyName("T")]
    public long TradeTime { get; init; }

    [JsonPropertyName("s")]
    public string Symbol { get; init; } = string.Empty;

    [JsonPropertyName("S")]
    public string Side { get; init; } = string.Empty;

    [JsonPropertyName("v")]
    public string Size { get; init; } = string.Empty;

    [JsonPropertyName("p")]
    public string Price { get; init; } = string.Empty;

    [JsonPropertyName("i")]
    public string TradeId { get; init; } = string.Empty;

    [JsonPropertyName("BT")]
    public bool IsBlockTrade { get; init; }

    [JsonPropertyName("RPI")]
    public bool IsRpiTrade { get; init; }

    [JsonPropertyName("seq")]
    public string? Sequence { get; init; }
}
