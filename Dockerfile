FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["CryptoCollector.Api/CryptoCollector.Api.csproj", "CryptoCollector.Api/"]
COPY ["CryptoCollector.API.Exchange/CryptoCollector.API.Exchange.csproj", "CryptoCollector.API.Exchange/"]
COPY ["CryptoCollector.Exchange.Binance/CryptoCollector.Exchange.Binance.csproj", "CryptoCollector.Exchange.Binance/"]
COPY ["CryptoCollector.Exchange.Bybit/CryptoCollector.Exchange.Bybit.csproj", "CryptoCollector.Exchange.Bybit/"]
COPY ["CryptoCollector.Exchange.Deribit/CryptoCollector.Exchange.Deribit.csproj", "CryptoCollector.Exchange.Deribit/"]
RUN dotnet restore "CryptoCollector.Api/CryptoCollector.Api.csproj"

COPY . .
RUN dotnet publish "CryptoCollector.Api/CryptoCollector.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends fontconfig fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

COPY --from=build /app/publish .
RUN mkdir -p /data

ENTRYPOINT ["dotnet", "CryptoCollector.Api.dll"]
