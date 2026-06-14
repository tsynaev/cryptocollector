using System.Collections.Immutable;
using CryptoCollector.API.Exchange.Models;

namespace CryptoCollector.API.Exchange.Services;

public sealed class InstrumentCatalog
{
    private ImmutableDictionary<string, InstrumentDefinition> _instruments = ImmutableDictionary<string, InstrumentDefinition>.Empty.WithComparers(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyCollection<InstrumentDefinition> All => _instruments.Values.ToArray();

    public bool Replace(IReadOnlyCollection<InstrumentDefinition> instruments)
    {
        var next = instruments
            .DistinctBy(static instrument => instrument.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToImmutableDictionary(static instrument => instrument.Symbol, StringComparer.OrdinalIgnoreCase);

        var changed = !_instruments.Keys.Order(StringComparer.OrdinalIgnoreCase).SequenceEqual(next.Keys.Order(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
        Interlocked.Exchange(ref _instruments, next);
        return changed;
    }

    public bool TryGet(string symbol, out InstrumentDefinition? instrument) => _instruments.TryGetValue(symbol, out instrument);

    public IReadOnlyList<InstrumentDefinition> GetByCategory(string category) =>
        _instruments.Values
            .Where(x => x.Category.Equals(category, StringComparison.OrdinalIgnoreCase))
            .OrderBy(x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();
}
