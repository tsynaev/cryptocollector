namespace CryptoCollector.Api.Options;

public sealed class StorageOptions
{
    public const string SectionName = "Storage";

    public string DataRoot { get; init; } = "data";
}
