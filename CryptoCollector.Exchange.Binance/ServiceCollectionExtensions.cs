using Binance.Net.Clients;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.Exchange.Binance.Options;
using CryptoCollector.Exchange.Binance.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CryptoCollector.Exchange.Binance;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddBinanceExchange(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<BinanceCollectorOptions>(configuration.GetSection(BinanceCollectorOptions.SectionName));
        services.AddSingleton(_ => new BinanceRestClient());
        services.AddSingleton(_ => new BinanceSocketClient());
        services.AddHttpClient<BinanceApiClient>((serviceProvider, client) =>
        {
            var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<BinanceCollectorOptions>>().Value;
            client.BaseAddress = new Uri(options.OptionsRestBaseUrl);
        });
        services.AddSingleton<BinanceExchange>();
        services.AddSingleton<IExchange>(sp => sp.GetRequiredService<BinanceExchange>());
        return services;
    }
}
