namespace CryptoCollector.API.Exchange.Models;

public abstract record ExchangeDataMessage(InstrumentDefinition Instrument);

public sealed record ExchangeTradeMessage(InstrumentDefinition Instrument, ExchangeTrade Trade) : ExchangeDataMessage(Instrument);

public sealed record ExchangeTickerMessage(InstrumentDefinition Instrument, ExchangeTicker Ticker) : ExchangeDataMessage(Instrument);

public sealed record ExchangeOptionMessage(InstrumentDefinition Instrument, ExchangeOptionTicker OptionTicker) : ExchangeDataMessage(Instrument);
