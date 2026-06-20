using System.Text.Json.Serialization;

namespace CryptoCollector.API.Exchange.Models;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum InstrumentType
{
    Unknown = 0,
    Spot = 1,
    Margin = 2,
    InversePerpetual = 10,
    LinearPerpetual = 11,
    LinearFutures = 12,
    InverseFutures = 13,
    Option = 20,
    Cash = 30
}
