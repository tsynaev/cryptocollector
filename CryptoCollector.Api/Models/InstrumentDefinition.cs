using Bybit.Net.Enums;
using Bybit.Net.Objects.Models.V5;
using System.Globalization;
using System.Text.RegularExpressions;

namespace CryptoCollector.Api.Models;

public sealed class InstrumentDefinition
{
    private static readonly Regex OptionSymbolRegex = new(
        "^(?<base>[A-Z]+)-(?<expiry>\\d{1,2}[A-Z]{3}\\d{2})-(?<strike>\\d+(?:\\.\\d+)?)-(?<type>[CP])(?:-(?<settle>[A-Z]+))?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public required string Exchange { get; init; }
    public required string Category { get; init; }
    public required string MarketType { get; init; }
    public required string Symbol { get; init; }
    public required string BaseAsset { get; init; }
    public required string QuoteAsset { get; init; }
    public required string SettleAsset { get; init; }
    public DateTime? ExpiryUtc { get; init; }
    public decimal? StrikePrice { get; init; }
    public string? OptionSide { get; init; }

    public static InstrumentDefinition FromBybit(BybitOptionSymbol source) =>
        new()
        {
            Exchange = "bybit",
            Category = "option",
            MarketType = "option",
            Symbol = source.Name,
            BaseAsset = source.BaseAsset,
            QuoteAsset = source.QuoteAsset,
            SettleAsset = source.SettleAsset,
            ExpiryUtc = source.DeliveryTime,
            StrikePrice = TryParseOptionStrike(source.Name),
            OptionSide = source.OptionType == OptionType.Call ? "Call" : "Put"
        };

    public static InstrumentDefinition FromBybit(BybitLinearInverseSymbol source) =>
        new()
        {
            Exchange = "bybit",
            Category = "linear",
            MarketType = source.ContractType.ToString().Contains("Perpetual", StringComparison.OrdinalIgnoreCase) ? "perpetual" : "future",
            Symbol = source.Name,
            BaseAsset = source.BaseAsset,
            QuoteAsset = source.QuoteAsset,
            SettleAsset = source.SettleAsset,
            ExpiryUtc = source.DeliveryTime,
            StrikePrice = null,
            OptionSide = null
        };

    private static decimal? TryParseOptionStrike(string symbol)
    {
        var optionMatch = OptionSymbolRegex.Match(symbol);
        if (!optionMatch.Success)
        {
            return null;
        }

        return decimal.TryParse(optionMatch.Groups["strike"].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var strikeValue)
            ? strikeValue
            : null;
    }
}
