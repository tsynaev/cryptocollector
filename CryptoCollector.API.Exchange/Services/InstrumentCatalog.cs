using System.Collections.Immutable;
using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.API.Exchange.Services;

public sealed class InstrumentCatalog
{
    private ImmutableDictionary<string, InstrumentDefinition> _instruments = ImmutableDictionary<string, InstrumentDefinition>.Empty.WithComparers(StringComparer.OrdinalIgnoreCase);

    private static string GetKey(string exchange, string symbol) => $"{exchange}|{symbol}";

    public IReadOnlyCollection<InstrumentDefinition> All => _instruments.Values.ToArray();

    public bool Replace(string exchange, IReadOnlyCollection<InstrumentDefinition> instruments)
    {
        var next = instruments
            .DistinctBy(static instrument => instrument.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToImmutableDictionary(
                instrument => GetKey(exchange, instrument.Symbol),
                static instrument => instrument,
                StringComparer.OrdinalIgnoreCase);

        var preserved = _instruments
            .Where(entry => !entry.Value.Exchange.Equals(exchange, StringComparison.OrdinalIgnoreCase))
            .ToImmutableDictionary(static entry => entry.Key, static entry => entry.Value, StringComparer.OrdinalIgnoreCase);

        next = preserved.SetItems(next);

        var changed = !_instruments.Keys.Order(StringComparer.OrdinalIgnoreCase).SequenceEqual(next.Keys.Order(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
        Interlocked.Exchange(ref _instruments, next);
        return changed;
    }

    public bool TryGet(string exchange, string symbol, out InstrumentDefinition? instrument) =>
        _instruments.TryGetValue(GetKey(exchange, symbol), out instrument);

    public IReadOnlyList<InstrumentDefinition> GetByCategory(string exchange, string category) =>
        _instruments.Values
            .Where(x => x.Exchange.Equals(exchange, StringComparison.OrdinalIgnoreCase))
            .Where(x => x.Category.Equals(category, StringComparison.OrdinalIgnoreCase))
            .OrderBy(x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();
}
