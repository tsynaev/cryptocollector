using System.Text.Json.Serialization;

namespace CryptoCollector.Exchange.Binance.Models;

internal sealed class BinanceOptionsExchangeInfoResponse
{
    public IReadOnlyList<BinanceOptionSymbol> OptionSymbols { get; init; } = [];
}

internal sealed class BinanceOptionSymbol
{
    public required long ExpiryDate { get; init; }
    public required string Symbol { get; init; }
    public required string Side { get; init; }
    public required string StrikePrice { get; init; }
    public required string Underlying { get; init; }
    public required string QuoteAsset { get; init; }
    public required string Status { get; init; }
}

internal sealed class BinanceOptionTickerStats
{
    public required string Symbol { get; init; }
    public required string LastPrice { get; init; }
    public required string Volume { get; init; }
    public required string Amount { get; init; }
    public required string BidPrice { get; init; }
    public required string AskPrice { get; init; }
    public required string PriceChangePercent { get; init; }
}

internal sealed class BinanceOptionMarkPrice
{
    public required string Symbol { get; init; }
    public required string MarkPrice { get; init; }
    public required string BidIV { get; init; }
    public required string AskIV { get; init; }
    public required string MarkIV { get; init; }
    public required string Delta { get; init; }
    public required string Gamma { get; init; }
    public required string Vega { get; init; }
    public required string Theta { get; init; }
}

internal sealed class BinanceOptionIndexPrice
{
    public required long Time { get; init; }
    public required string IndexPrice { get; init; }
}

internal sealed class BinanceOptionRecentTrade
{
    public required long Id { get; init; }
    public required long TradeId { get; init; }
    public required string Symbol { get; init; }
    public required string Price { get; init; }
    public required string Qty { get; init; }
    public required string QuoteQty { get; init; }
    public required int Side { get; init; }
    public required long Time { get; init; }
}

internal sealed class BinanceOptionTradeStreamMessage
{
    [JsonPropertyName("T")]
    public required long TradeTime { get; init; }

    [JsonPropertyName("s")]
    public required string Symbol { get; init; }

    [JsonPropertyName("t")]
    public required long TradeId { get; init; }

    [JsonPropertyName("p")]
    public required string Price { get; init; }

    [JsonPropertyName("q")]
    public required string Quantity { get; init; }

    [JsonPropertyName("X")]
    public string? TradeType { get; init; }

    [JsonPropertyName("S")]
    public string? Side { get; init; }

    [JsonPropertyName("m")]
    public bool BuyerIsMarketMaker { get; init; }
}
