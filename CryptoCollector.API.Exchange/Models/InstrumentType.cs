namespace CryptoCollector.API.Exchange.Models;

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
