using Azure.Data.Tables;
using CsvHelper;
using Medienstudio.Azure.Data.Tables.Extensions;
using System.Globalization;

namespace Medienstudio.Azure.Data.Tables.CSV;

/// <summary>
/// Provides extension methods for exporting and importing Azure Table Storage data as CSV.
/// </summary>
public static class Extensions
{
    const string TYPE_SUFFIX = "@type";
    static readonly string[] SYSTEM_PROPERTIES = ["PartitionKey", "RowKey", "Timestamp"];


    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <returns>Task<void></void></returns>
    public static async Task ExportCSVAsync(this TableClient tableClient, TextWriter writer)
    {
        CsvExportSchema schema = await tableClient.GetCSVExportSchemaAsync();
        await tableClient.ExportCSVAsync(writer, schema);
    }

    /// <summary>
    /// Returns the CSV export schema discovered from all entities in the table.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <returns>The non-system properties in the order they are first encountered.</returns>
    public static async Task<CsvExportSchema> GetCSVExportSchemaAsync(this TableClient tableClient)
    {
        HashSet<string> knownProperties = new(StringComparer.Ordinal);
        List<string> properties = [];

        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            AddProperties(row, knownProperties, properties);
        }

        return new CsvExportSchema(properties);
    }

    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer after buffering them in memory.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <returns>Task<void></void></returns>
    /// <remarks>Use this method only when the table fits comfortably in memory. It queries the table once and buffers every entity before writing.</remarks>
    public static async Task ExportCSVInMemoryAsync(this TableClient tableClient, TextWriter writer)
    {
        List<TableEntity> rows = [];
        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            rows.Add(row);
        }

        CsvExportSchema schema = CreateCSVExportSchema(rows);
        WriteCSV(writer, schema, rows);
    }

    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer using the supplied schema.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <param name="schema">The non-system properties to include in the CSV export.</param>
    /// <returns>Task<void></void></returns>
    /// <exception cref="InvalidOperationException">An entity contains a property that is absent from <paramref name="schema"/>.</exception>
    public static async Task ExportCSVAsync(this TableClient tableClient, TextWriter writer, CsvExportSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        List<string> keys = CreateCSVKeys(schema);
        HashSet<string> schemaProperties = schema.Properties.ToHashSet(StringComparer.Ordinal);

        using CsvWriter csv = CreateCSVWriter(writer);
        WriteCSVHeader(csv, keys);

        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            WriteCSVRow(csv, keys, schemaProperties, row);
        }

        csv.Flush();
    }

    private static CsvExportSchema CreateCSVExportSchema(IEnumerable<TableEntity> rows)
    {
        HashSet<string> knownProperties = new(StringComparer.Ordinal);
        List<string> properties = [];

        foreach (TableEntity row in rows)
        {
            AddProperties(row, knownProperties, properties);
        }

        return new CsvExportSchema(properties);
    }

    private static void AddProperties(TableEntity row, HashSet<string> knownProperties, List<string> properties)
    {
        foreach (KeyValuePair<string, object> property in row)
        {
            if (IsExportedProperty(property.Key) && knownProperties.Add(property.Key))
            {
                properties.Add(property.Key);
            }
        }
    }

    private static void WriteCSV(TextWriter writer, CsvExportSchema schema, IEnumerable<TableEntity> rows)
    {
        List<string> keys = CreateCSVKeys(schema);
        HashSet<string> schemaProperties = schema.Properties.ToHashSet(StringComparer.Ordinal);

        using CsvWriter csv = CreateCSVWriter(writer);
        WriteCSVHeader(csv, keys);

        foreach (TableEntity row in rows)
        {
            WriteCSVRow(csv, keys, schemaProperties, row);
        }

        csv.Flush();
    }

    private static List<string> CreateCSVKeys(CsvExportSchema schema)
    {
        List<string> keys = [.. SYSTEM_PROPERTIES];
        foreach (string property in schema.Properties)
        {
            keys.Add(property);
            keys.Add(property + TYPE_SUFFIX);
        }

        return keys;
    }

    private static CsvWriter CreateCSVWriter(TextWriter writer)
    {
        CsvWriter csv = new(writer, CultureInfo.InvariantCulture);

        // preserve milliseconds, truncate trailing zeros
        csv.Context.TypeConverterOptionsCache.GetOptions<DateTime>().Formats = ["yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"];
        csv.Context.TypeConverterCache.AddConverter<DateTimeOffset>(new UtcDateTimeOffsetConverter());


        // serialize byte arrays as base64 strings
        csv.Context.TypeConverterCache.AddConverter<byte[]>(new BinaryConverter());

        // serialize booleans lowercase
        csv.Context.TypeConverterCache.AddConverter<bool>(new BoolConverter());

        return csv;
    }

    private static void WriteCSVHeader(CsvWriter csv, IEnumerable<string> keys)
    {
        foreach (string key in keys)
        {
            csv.WriteField(key);
        }
        csv.NextRecord();
    }

    private static void WriteCSVRow(CsvWriter csv, IEnumerable<string> keys, HashSet<string> schemaProperties, TableEntity row)
    {
        foreach (KeyValuePair<string, object> property in row)
        {
            if (IsExportedProperty(property.Key) && !schemaProperties.Contains(property.Key))
            {
                throw new InvalidOperationException($"The property '{property.Key}' is not included in the CSV export schema.");
            }
        }

        foreach (string key in keys)
        {
            if (row.TryGetValue(key, out object value))
            {
                // write the value of the property
                csv.WriteField(value);
            }
            else if (key.EndsWith(TYPE_SUFFIX))
            {
                // write the type of the property
                if (row.ContainsKey(key[..^TYPE_SUFFIX.Length]))
                {
                    csv.WriteField(row[key[..^TYPE_SUFFIX.Length]].GetPropertyTypeName());
                }
                else
                {
                    csv.WriteField("");
                }
            }
            else
            {
                // write an empty field
                csv.WriteField("");
            }
        }
        csv.NextRecord();
    }

    private static bool IsExportedProperty(string propertyName)
    {
        return propertyName != "odata.etag" && !SYSTEM_PROPERTIES.Contains(propertyName);
    }


    /// <summary>
    /// Imports a CSV read stream to Table Storage
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="reader">TextReader instance providing access to the CSV</param>
    /// <returns></returns>
    public static async Task ImportCSVAsync(this TableClient tableClient, TextReader reader)
    {
        using CsvReader csv = new(reader, CultureInfo.InvariantCulture);
        csv.Read();
        csv.ReadHeader();
        List<TableEntity> entities = [];
        int batchCounter = 0;

        while (csv.Read())
        {
            batchCounter++;
            // loop through fields while index is not out of bounds
            TableEntity entity = [];
            int i = 0;
            while (csv.TryGetField(i, out string? field))
            {
                string? label = csv.HeaderRecord?[i];
                if (label == null)
                {
                    return;
                }

                if (SYSTEM_PROPERTIES.Contains(label))
                {
                    if (string.IsNullOrEmpty(field))
                    {
                        i++;
                        continue;
                    }

                    switch (label)
                    {
                        case "PartitionKey":
                            entity.PartitionKey = field;
                            break;
                        case "RowKey":
                            entity.RowKey = field;
                            break;
                        case "Timestamp":
                            entity.Timestamp = DateTimeOffset.Parse(field);
                            break;
                    }
                }
                else if (!label.EndsWith(TYPE_SUFFIX))
                {
                    string? type = csv.GetField<string>(label + "@type")?.Split('@')[0];
                    if (!string.IsNullOrEmpty(field) || type is "String" or "Binary")
                    {
                        object value = CoerceType(type, field ?? string.Empty);
                        entity.Add(label, value);
                    }
                }

                i++;
            }
            entities.Add(entity);

            if (batchCounter == 100)
            {
                await tableClient.AddEntitiesAsync(entities);
                entities = [];
                batchCounter = 0;
            }
        }
        if (entities.Count > 0)
        {
            await tableClient.AddEntitiesAsync(entities);
        }
    }

    private static object CoerceType(string? type, string field)
    {
        return type switch
        {
            "Boolean" => bool.Parse(field),
            "DateTime" => DateTimeOffset.Parse(field, CultureInfo.InvariantCulture),
            "Double" => double.Parse(field, CultureInfo.InvariantCulture),
            "Guid" => Guid.Parse(field),
            "Int32" or "int" => int.Parse(field, CultureInfo.InvariantCulture),
            "Int64" or "long" => long.Parse(field, CultureInfo.InvariantCulture),
            "Binary" => Convert.FromBase64String(field),
            _ => field,
        };
    }
}
