using CryptoCollector.API.Exchange.Abstractions;
using CryptoCollector.Exchange.Deribit.Options;
using CryptoCollector.Exchange.Deribit.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CryptoCollector.Exchange.Deribit;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDeribitExchange(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<DeribitCollectorOptions>(configuration.GetSection(DeribitCollectorOptions.SectionName));
        services.AddHttpClient<DeribitApiClient>((serviceProvider, client) =>
        {
            var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<DeribitCollectorOptions>>().Value;
            client.BaseAddress = new Uri(options.RestBaseUrl);
            client.Timeout = TimeSpan.FromSeconds(30);
        });
        services.AddSingleton<DeribitCollectorHostedService>();
        services.AddSingleton<IExchangeMarketDataClient>(sp => sp.GetRequiredService<DeribitApiClient>());
        services.AddSingleton<IExchangeCollector>(sp => sp.GetRequiredService<DeribitCollectorHostedService>());
        services.AddHostedService(sp => sp.GetRequiredService<DeribitCollectorHostedService>());
        return services;
    }
}
