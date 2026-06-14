using Microsoft.Extensions.Hosting;

namespace CryptoCollector.API.Exchange.Abstractions;

public interface IExchangeCollector : IHostedService
{
    string Exchange { get; }
}
