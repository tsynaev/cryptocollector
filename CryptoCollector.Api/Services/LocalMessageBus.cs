using System.Collections.Concurrent;

namespace CryptoCollector.Api.Services;

public sealed class LocalMessageBus : ILocalMessageBus
{
    private readonly Lock _gate = new();
    private readonly ConcurrentDictionary<Type, List<object>> _subscribers = new();

    public void Publish<T>(T message) where T : class
    {
        if (!_subscribers.TryGetValue(typeof(T), out var subscribers))
        {
            return;
        }

        List<object> snapshot;
        lock (_gate)
        {
            snapshot = subscribers.ToList();
        }

        foreach (var subscriber in snapshot.OfType<Func<T, Task>>())
        {
            _ = subscriber(message);
        }
    }

    public IDisposable Subscribe<T>(Func<T, Task> handler) where T : class
    {
        var subscribers = _subscribers.GetOrAdd(typeof(T), static _ => []);
        lock (_gate)
        {
            subscribers.Add(handler);
        }

        return new Subscription(() =>
        {
            lock (_gate)
            {
                if (_subscribers.TryGetValue(typeof(T), out var current))
                {
                    current.Remove(handler!);
                    if (current.Count == 0)
                    {
                        _subscribers.TryRemove(typeof(T), out _);
                    }
                }
            }
        });
    }

    private sealed class Subscription(Action dispose) : IDisposable
    {
        private Action? _dispose = dispose;

        public void Dispose()
        {
            Interlocked.Exchange(ref _dispose, null)?.Invoke();
        }
    }
}
