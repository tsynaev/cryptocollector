FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["CryptoCollector.Api.csproj", "./"]
RUN dotnet restore "CryptoCollector.Api.csproj"

COPY . .
RUN dotnet publish "CryptoCollector.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

ENV ASPNETCORE_URLS=http://+:8080
ENV DOTNET_SYSTEM_NET_DISABLEIPV6=1
EXPOSE 8080

COPY --from=build /app/publish .
RUN mkdir -p /data

ENTRYPOINT ["dotnet", "CryptoCollector.Api.dll"]
