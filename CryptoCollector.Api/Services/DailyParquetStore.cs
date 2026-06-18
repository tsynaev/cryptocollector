using CryptoCollector.Api.Models;
using CryptoCollector.Api.Options;
using Microsoft.Extensions.Options;
using Parquet;
using Parquet.Serialization;

namespace CryptoCollector.Api.Services;

public sealed class DailyParquetStore(IOptions<StorageOptions> options)
{
    private readonly string _dataRoot = Path.GetFullPath(options.Value.DataRoot);
    private readonly ParquetOptions _parquetOptions = new()
    {
        CompressionMethod = CompressionMethod.Zstd
    };

    public async Task AppendAsync<T>(
        string exchange,
        string dataSet,
        IReadOnlyCollection<T> rows,
        CancellationToken cancellationToken) where T : class, ITimeSeriesRecord
    {
        if (rows.Count == 0)
        {
            return;
        }

        var byDay = rows.GroupBy(static x => DateOnly.FromDateTime(x.Date));

        foreach (var group in byDay)
        {
            var directory = Path.Combine(_dataRoot, exchange.ToLowerInvariant(), dataSet);
            Directory.CreateDirectory(directory);

            var filePath = Path.Combine(directory, $"{group.Key:yyyy-MM-dd}.parquet");
            var writeOptions = new ParquetOptions
            {
                CompressionMethod = _parquetOptions.CompressionMethod,
                Append = File.Exists(filePath)
            };

            var orderedRows = group.OrderBy(static x => x.Date).ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase).ToArray();

            try
            {
                await ParquetSerializer.SerializeAsync(
                    orderedRows,
                    filePath,
                    writeOptions,
                    cancellationToken: cancellationToken);
            }
            catch (ParquetException) when (File.Exists(filePath))
            {
                await MigrateAndRewriteAsync(filePath, orderedRows, cancellationToken);
            }
        }
    }

    public async Task<IReadOnlyList<T>> QueryAsync<T>(
        string exchange,
        string dataSet,
        DateTime fromUtc,
        DateTime toUtc,
        string? symbol,
        CancellationToken cancellationToken) where T : class, ITimeSeriesRecord, new()
    {
        var directory = Path.Combine(_dataRoot, exchange.ToLowerInvariant(), dataSet);
        if (!Directory.Exists(directory))
        {
            return [];
        }

        var results = new List<T>();
        var current = DateOnly.FromDateTime(fromUtc.Date);
        var end = DateOnly.FromDateTime(toUtc.Date);

        while (current <= end)
        {
            var filePath = Path.Combine(directory, $"{current:yyyy-MM-dd}.parquet");
            if (File.Exists(filePath))
            {
                foreach (var row in await ReadRowsAsync<T>(filePath, cancellationToken))
                {
                    if (row.Date < fromUtc || row.Date > toUtc)
                    {
                        continue;
                    }

                    if (!string.IsNullOrWhiteSpace(symbol) && !row.Symbol.Equals(symbol, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    results.Add(row);
                }
            }

            current = current.AddDays(1);
        }

        return results
            .OrderBy(static x => x.Date)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<IReadOnlyList<T>> QueryLatestAsync<T>(
        string exchange,
        string dataSet,
        string? symbol,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken) where T : class, ITimeSeriesRecord, new()
    {
        var directory = Path.Combine(_dataRoot, exchange.ToLowerInvariant(), dataSet);
        if (!Directory.Exists(directory))
        {
            return [];
        }

        var files = Directory
            .EnumerateFiles(directory, "*.parquet", SearchOption.TopDirectoryOnly)
            .OrderByDescending(static path => path, StringComparer.OrdinalIgnoreCase);

        DateTime? latestDate = null;
        var results = new List<T>();

        foreach (var filePath in files)
        {
            foreach (var row in await ReadRowsAsync<T>(filePath, cancellationToken))
            {
                if (!string.IsNullOrWhiteSpace(symbol) && !row.Symbol.Equals(symbol, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (predicate is not null && !predicate(row))
                {
                    continue;
                }

                if (latestDate is null || row.Date > latestDate.Value)
                {
                    latestDate = row.Date;
                    results.Clear();
                    results.Add(row);
                    continue;
                }

                if (row.Date == latestDate.Value)
                {
                    results.Add(row);
                }
            }

            if (latestDate is not null)
            {
                break;
            }
        }

        return results
            .OrderBy(static x => x.Date)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<DateTime?> GetLatestTimestampAsync<T>(
        string exchange,
        string dataSet,
        CancellationToken cancellationToken) where T : class, ITimeSeriesRecord, new()
    {
        var latest = await QueryLatestAsync<T>(exchange, dataSet, symbol: null, predicate: null, cancellationToken);
        return latest.Count == 0 ? null : latest.Max(static x => x.Date);
    }

    private async Task<IReadOnlyList<T>> ReadRowsAsync<T>(string filePath, CancellationToken cancellationToken)
        where T : class, ITimeSeriesRecord, new()
    {
        try
        {
            var rows = await ParquetSerializer.DeserializeAsync<T>(
                filePath,
                _parquetOptions,
                cancellationToken: cancellationToken);

            return rows.Data.ToArray();
        }
        catch (ParquetException) when (typeof(T) == typeof(TradeRecord))
        {
            var legacyRows = await ParquetSerializer.DeserializeAsync<LegacyTradeRecordV1>(
                filePath,
                _parquetOptions,
                cancellationToken: cancellationToken);

            return legacyRows.Data
                .Select(static x => (T)(ITimeSeriesRecord)x.Upgrade())
                .ToArray();
        }
    }

    private async Task MigrateAndRewriteAsync<T>(string filePath, IReadOnlyCollection<T> newRows, CancellationToken cancellationToken)
        where T : class, ITimeSeriesRecord
    {
        if (typeof(T) != typeof(TradeRecord))
        {
            throw new ParquetException($"Schema migration is not implemented for {typeof(T).Name}.");
        }

        var legacyRows = await ParquetSerializer.DeserializeAsync<LegacyTradeRecordV1>(
            filePath,
            _parquetOptions,
            cancellationToken: cancellationToken);

        var mergedRows = legacyRows.Data
            .Select(static x => x.Upgrade())
            .Concat(newRows.Cast<TradeRecord>())
            .OrderBy(static x => x.Date)
            .ThenBy(static x => x.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        await ParquetSerializer.SerializeAsync(
            mergedRows,
            filePath,
            new ParquetOptions
            {
                CompressionMethod = _parquetOptions.CompressionMethod,
                Append = false
            },
            cancellationToken: cancellationToken);
    }
}
