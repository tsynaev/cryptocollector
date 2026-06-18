using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.API.Exchange.Services;

public interface IMarketDataSink
{
    void IngestTrade(InstrumentDefinition instrument, ExchangeTrade trade);
    void IngestTicker(InstrumentDefinition instrument, ExchangeTicker ticker);
    void IngestOption(InstrumentDefinition instrument, ExchangeOptionTicker optionTicker);
}
