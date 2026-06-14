using Bybit.Net.Objects.Models.V5;
using CryptoCollector.API.Exchange.Models;
using System.Text.Json;

namespace CryptoCollector.API.Exchange.Services;

public interface IMarketDataSink
{
    void IngestTrade(InstrumentDefinition instrument, ExchangeTrade trade);
    void IngestTicker(InstrumentDefinition instrument, JsonElement payload, DateTimeOffset eventTimestamp);
    void IngestTrade(InstrumentDefinition instrument, BybitTrade trade);
    void IngestTrade(InstrumentDefinition instrument, BybitTradeHistory trade);
    void IngestTrade(InstrumentDefinition instrument, BybitOptionTrade trade);
    void IngestTicker(InstrumentDefinition instrument, BybitLinearTickerUpdate payload, DateTimeOffset eventTimestamp);
    void IngestTicker(InstrumentDefinition instrument, BybitLinearInverseTicker payload, DateTimeOffset eventTimestamp);
    void IngestTicker(InstrumentDefinition instrument, BybitOptionTickerUpdate payload, DateTimeOffset eventTimestamp);
    void IngestTicker(InstrumentDefinition instrument, BybitOptionTicker payload, DateTimeOffset eventTimestamp);
}
