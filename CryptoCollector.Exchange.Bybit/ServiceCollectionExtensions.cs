using Bybit.Net.Clients;
using CryptoExchange.Net.Objects;
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
        services.AddSingleton(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<BybitCollectorOptions>>().Value;
            return new BybitSocketClient(socketOptions =>
            {
                socketOptions.ReconnectPolicy = ReconnectPolicy.FixedDelay;
                socketOptions.ReconnectInterval = options.ReconnectDelay;
                socketOptions.V5Options.PingInterval = options.HeartbeatInterval;
            });
        });
        services.AddSingleton<BybitApiClient>();
        services.AddSingleton<BybitExchange>();
        services.AddSingleton<IExchange>(sp => sp.GetRequiredService<BybitExchange>());
        return services;
    }
}
