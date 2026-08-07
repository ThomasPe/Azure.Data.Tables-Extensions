namespace Medienstudio.Azure.Data.Tables.CSV;

/// <summary>
/// Describes the non-system properties included in a CSV export.
/// </summary>
public sealed class CsvExportSchema
{
    private static readonly string[] ReservedPropertyNames = ["PartitionKey", "RowKey", "Timestamp", "odata.etag"];

    /// <summary>
    /// Initializes a new instance of the <see cref="CsvExportSchema"/> class.
    /// </summary>
    /// <param name="properties">The non-system property names to include in the CSV export.</param>
    public CsvExportSchema(IEnumerable<string> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);

        string[] propertyNames = properties.ToArray();
        if (propertyNames.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("Property names cannot be null, empty, or whitespace.", nameof(properties));
        }

        if (propertyNames.Distinct(StringComparer.Ordinal).Count() != propertyNames.Length)
        {
            throw new ArgumentException("Property names must be unique.", nameof(properties));
        }

        if (propertyNames.Any(propertyName => ReservedPropertyNames.Contains(propertyName, StringComparer.Ordinal)))
        {
            throw new ArgumentException("System properties cannot be included in a CSV export schema.", nameof(properties));
        }

        Properties = Array.AsReadOnly(propertyNames);
    }

    /// <summary>
    /// Gets the non-system property names in CSV column order.
    /// </summary>
    public IReadOnlyList<string> Properties { get; }
}