using Bybit.Net.Clients;
using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.Exchange.Bybit.Options;
using CryptoCollector.Exchange.Bybit.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CryptoCollector.Exchange.Bybit;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddBybitExchange(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<BybitCollectorOptions>(configuration.GetSection(BybitCollectorOptions.SectionName));
        services.AddSingleton(_ => new BybitRestClient());
        services.AddSingleton(_ => new BybitSocketClient());
        services.AddSingleton<BybitApiClient>();
        services.AddSingleton<BybitCollectorHostedService>();
        services.AddSingleton<IExchangeMarketDataClient>(sp => sp.GetRequiredService<BybitApiClient>());
        services.AddSingleton<IExchangeCollector>(sp => sp.GetRequiredService<BybitCollectorHostedService>());
        services.AddHostedService(sp => sp.GetRequiredService<BybitCollectorHostedService>());
        return services;
    }
}
