using System.Text.Json.Serialization;

namespace CryptoCollector.Exchange.Deribit.Models;

public sealed class DeribitRpcResponse<T>
{
    [JsonPropertyName("jsonrpc")]
    public string JsonRpc { get; init; } = string.Empty;

    [JsonPropertyName("id")]
    public long? Id { get; init; }

    [JsonPropertyName("result")]
    public T? Result { get; init; }

    [JsonPropertyName("error")]
    public DeribitRpcError? Error { get; init; }
}

public sealed class DeribitRpcError
{
    [JsonPropertyName("code")]
    public int Code { get; init; }

    [JsonPropertyName("message")]
    public string Message { get; init; } = string.Empty;
}

public sealed class DeribitInstrument
{
    [JsonPropertyName("instrument_name")]
    public string InstrumentName { get; init; } = string.Empty;

    [JsonPropertyName("kind")]
    public string Kind { get; init; } = string.Empty;

    [JsonPropertyName("base_currency")]
    public string BaseCurrency { get; init; } = string.Empty;

    [JsonPropertyName("quote_currency")]
    public string QuoteCurrency { get; init; } = string.Empty;

    [JsonPropertyName("settlement_currency")]
    public string? SettlementCurrency { get; init; }

    [JsonPropertyName("counter_currency")]
    public string? CounterCurrency { get; init; }

    [JsonPropertyName("is_active")]
    public bool IsActive { get; init; }

    [JsonPropertyName("expiration_timestamp")]
    public long ExpirationTimestamp { get; init; }

    [JsonPropertyName("strike")]
    public decimal? Strike { get; init; }

    [JsonPropertyName("option_type")]
    public string? OptionType { get; init; }

    [JsonPropertyName("settlement_period")]
    public string? SettlementPeriod { get; init; }
}

public sealed class DeribitBookSummary
{
    [JsonPropertyName("instrument_name")]
    public string InstrumentName { get; init; } = string.Empty;

    [JsonPropertyName("base_currency")]
    public string BaseCurrency { get; init; } = string.Empty;

    [JsonPropertyName("quote_currency")]
    public string QuoteCurrency { get; init; } = string.Empty;

    [JsonPropertyName("open_interest")]
    public decimal? OpenInterest { get; init; }

    [JsonPropertyName("mark_price")]
    public decimal? MarkPrice { get; init; }

    [JsonPropertyName("last")]
    public decimal? Last { get; init; }

    [JsonPropertyName("bid_price")]
    public decimal? BidPrice { get; init; }

    [JsonPropertyName("ask_price")]
    public decimal? AskPrice { get; init; }

    [JsonPropertyName("mid_price")]
    public decimal? MidPrice { get; init; }

    [JsonPropertyName("underlying_price")]
    public decimal? UnderlyingPrice { get; init; }

    [JsonPropertyName("interest_rate")]
    public decimal? InterestRate { get; init; }

    [JsonPropertyName("volume")]
    public decimal? Volume { get; init; }

    [JsonPropertyName("creation_timestamp")]
    public long? CreationTimestamp { get; init; }

    [JsonPropertyName("price_change")]
    public decimal? PriceChange { get; init; }
}

public sealed class DeribitTradeBatch
{
    [JsonPropertyName("trades")]
    public IReadOnlyList<DeribitTrade> Trades { get; init; } = [];

    [JsonPropertyName("has_more")]
    public bool HasMore { get; init; }
}

public sealed class DeribitTrade
{
    [JsonPropertyName("trade_id")]
    public string TradeId { get; init; } = string.Empty;

    [JsonPropertyName("trade_seq")]
    public long TradeSequence { get; init; }

    [JsonPropertyName("instrument_name")]
    public string InstrumentName { get; init; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public long Timestamp { get; init; }

    [JsonPropertyName("direction")]
    public string Direction { get; init; } = string.Empty;

    [JsonPropertyName("price")]
    public decimal Price { get; init; }

    [JsonPropertyName("mark_price")]
    public decimal? MarkPrice { get; init; }

    [JsonPropertyName("index_price")]
    public decimal? IndexPrice { get; init; }

    [JsonPropertyName("iv")]
    public decimal? Iv { get; init; }

    [JsonPropertyName("amount")]
    public decimal Amount { get; init; }

    [JsonPropertyName("contracts")]
    public decimal? Contracts { get; init; }

    [JsonPropertyName("tick_direction")]
    public int? TickDirection { get; init; }

    [JsonPropertyName("block_trade_id")]
    public string? BlockTradeId { get; init; }

    [JsonPropertyName("block_trade_leg_count")]
    public int? BlockTradeLegCount { get; init; }

    [JsonPropertyName("combo_id")]
    public string? ComboId { get; init; }

    [JsonPropertyName("combo_trade_id")]
    public string? ComboTradeId { get; init; }

    [JsonPropertyName("block_rfq_id")]
    public long? BlockRfqId { get; init; }

    [JsonPropertyName("liquidation")]
    public string? Liquidation { get; init; }
}

public sealed class DeribitSubscriptionMessage
{
    [JsonPropertyName("jsonrpc")]
    public string? JsonRpc { get; init; }

    [JsonPropertyName("method")]
    public string? Method { get; init; }

    [JsonPropertyName("params")]
    public DeribitSubscriptionParams? Params { get; init; }
}

public sealed class DeribitSubscriptionParams
{
    [JsonPropertyName("channel")]
    public string Channel { get; init; } = string.Empty;

    [JsonPropertyName("data")]
    public System.Text.Json.JsonElement Data { get; init; }
}
