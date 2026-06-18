using CryptoCollector.API.Exchange.Models;
using CryptoCollector.API.Exchange.Services;

namespace CryptoCollector.Api.Services;

public sealed class CompositeMarketDataSink(
    MinuteAggregationService minuteAggregationService) : IMarketDataSink, IFlushableMarketDataSink
{
    public void IngestTrade(InstrumentDefinition instrument, ExchangeTrade trade)
    {
        minuteAggregationService.IngestTrade(instrument, trade);
    }

    public void IngestTicker(InstrumentDefinition instrument, ExchangeTicker ticker)
    {
        minuteAggregationService.IngestTicker(instrument, ticker);
    }

    public void IngestOption(InstrumentDefinition instrument, ExchangeOptionTicker optionTicker)
    {
        minuteAggregationService.IngestOption(instrument, optionTicker);
    }

    public Task FlushPendingAsync(CancellationToken cancellationToken) =>
        minuteAggregationService.FlushPendingAsync(cancellationToken);
}
