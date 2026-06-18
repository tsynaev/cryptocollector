using System.Text.Json;
using CryptoCollector.Api.Options;
using Microsoft.Extensions.Options;

namespace CryptoCollector.Api.Services;

public sealed class ServiceStateStore(IOptions<StorageOptions> options)
{
    private const int ReplaceRetryCount = 5;
    private static readonly TimeSpan ReplaceRetryDelay = TimeSpan.FromMilliseconds(50);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    private readonly string _stateDirectory = Path.Combine(Path.GetFullPath(options.Value.DataRoot), "_state");
    private readonly Lock _gate = new();

    public async Task<T?> ReadAsync<T>(string key, CancellationToken cancellationToken) where T : class
    {
        var path = GetPath(key);
        lock (_gate)
        {
            if (!File.Exists(path))
            {
                return null;
            }
        }

        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read);

        return await JsonSerializer.DeserializeAsync<T>(stream, JsonOptions, cancellationToken);
    }

    public async Task WriteAsync<T>(string key, T value, CancellationToken cancellationToken) where T : class
    {
        var path = GetPath(key);
        string tempPath;

        lock (_gate)
        {
            Directory.CreateDirectory(_stateDirectory);
            tempPath = Path.Combine(_stateDirectory, $"{key}.{Guid.NewGuid():N}.tmp");
        }

        try
        {
            await using (var stream = new FileStream(
                tempPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None))
            {
                await JsonSerializer.SerializeAsync(stream, value, JsonOptions, cancellationToken);
                await stream.FlushAsync(cancellationToken);
            }

            ReplaceFileWithRetry(tempPath, path, cancellationToken);
        }
        finally
        {
            TryDeleteTempFile(tempPath);
        }
    }

    private string GetPath(string key) => Path.Combine(_stateDirectory, $"{key}.json");

    private void ReplaceFileWithRetry(string tempPath, string path, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                lock (_gate)
                {
                    if (File.Exists(path))
                    {
                        File.Replace(tempPath, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
                    }
                    else
                    {
                        File.Move(tempPath, path);
                    }
                }

                return;
            }
            catch (UnauthorizedAccessException) when (attempt < ReplaceRetryCount)
            {
                Thread.Sleep(ReplaceRetryDelay);
            }
            catch (IOException) when (attempt < ReplaceRetryCount)
            {
                Thread.Sleep(ReplaceRetryDelay);
            }
        }
    }

    private static void TryDeleteTempFile(string tempPath)
    {
        try
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
        catch
        {
        }
    }
}
