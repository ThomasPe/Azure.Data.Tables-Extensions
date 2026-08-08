using Azure.Data.Tables;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Medienstudio.Azure.Data.Tables.JSON;

/// <summary>
/// Provides extension methods for exporting and importing Azure Table Storage data as newline-delimited JSON (NDJSON).
/// </summary>
public static class Extensions
{
    const string TYPE_SUFFIX = "@type";
    const string DATETIME_FORMAT = "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ";
    static readonly string[] SYSTEM_PROPERTIES = ["PartitionKey", "RowKey", "Timestamp"];

    /// <summary>
    /// Returns all rows in the table as newline-delimited JSON to the provided writer.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <returns>Task<void></void></returns>
    public static async Task ExportJSONAsync(this TableClient tableClient, TextWriter writer)
    {
        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            await WriteJSONLineAsync(writer, row);
        }
    }

    /// <summary>
    /// Returns all rows in the table as newline-delimited JSON to the provided writer after buffering them in memory.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <returns>Task<void></void></returns>
    /// <remarks>Use this method only when the table fits comfortably in memory. It queries the table once and buffers every entity before writing.</remarks>
    public static async Task ExportJSONInMemoryAsync(this TableClient tableClient, TextWriter writer)
    {
        List<TableEntity> rows = [];
        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            rows.Add(row);
        }

        foreach (TableEntity row in rows)
        {
            await WriteJSONLineAsync(writer, row);
        }
    }

    private static async Task WriteJSONLineAsync(TextWriter writer, TableEntity row)
    {
        JsonObject json = [];
        json["PartitionKey"] = row.PartitionKey;
        json["RowKey"] = row.RowKey;
        json["Timestamp"] = row.Timestamp?.UtcDateTime.ToString(DATETIME_FORMAT, CultureInfo.InvariantCulture);

        foreach (KeyValuePair<string, object> property in row)
        {
            if (!IsExportedProperty(property.Key))
            {
                continue;
            }

            json[property.Key] = ToJsonValue(property.Value);
            json[property.Key + TYPE_SUFFIX] = property.Value.GetPropertyTypeName();
        }

        // write an explicit LF regardless of TextWriter.NewLine so output bytes are stable across platforms
        await writer.WriteAsync(json.ToJsonString());
        await writer.WriteAsync('\n');
    }

    private static JsonValue? ToJsonValue(object value)
    {
        return value switch
        {
            byte[] bytes => JsonValue.Create(Convert.ToBase64String(bytes)),
            bool boolean => JsonValue.Create(boolean),
            DateTime dateTime => JsonValue.Create(dateTime.ToUniversalTime().ToString(DATETIME_FORMAT, CultureInfo.InvariantCulture)),
            DateTimeOffset dateTimeOffset => JsonValue.Create(dateTimeOffset.UtcDateTime.ToString(DATETIME_FORMAT, CultureInfo.InvariantCulture)),
            double number => JsonValue.Create(number),
            Guid guid => JsonValue.Create(guid.ToString()),
            int int32 => JsonValue.Create(int32),
            long int64 => JsonValue.Create(int64),
            string text => JsonValue.Create(text),
            _ => JsonValue.Create(value.ToString()),
        };
    }

    private static bool IsExportedProperty(string propertyName)
    {
        return propertyName != "odata.etag" && !SYSTEM_PROPERTIES.Contains(propertyName);
    }

    /// <summary>
    /// Imports a newline-delimited JSON read stream to Table Storage
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="reader">TextReader instance providing access to the NDJSON</param>
    /// <returns></returns>
    public static async Task ImportJSONAsync(this TableClient tableClient, TextReader reader)
    {
        List<TableEntity> entities = [];
        int batchCounter = 0;

        string? line;
        while ((line = await reader.ReadLineAsync()) is not null)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            entities.Add(ParseJSONLine(line));
            batchCounter++;

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

    private static TableEntity ParseJSONLine(string line)
    {
        JsonObject json = JsonNode.Parse(line)?.AsObject() ?? throw new InvalidOperationException("Expected a JSON object per line.");
        TableEntity entity = [];

        foreach (KeyValuePair<string, JsonNode?> property in json)
        {
            if (property.Key.EndsWith(TYPE_SUFFIX, StringComparison.Ordinal))
            {
                continue;
            }

            if (SYSTEM_PROPERTIES.Contains(property.Key))
            {
                ApplySystemProperty(entity, property.Key, property.Value);
                continue;
            }

            string? type = json[property.Key + TYPE_SUFFIX]?.GetValue<string>();
            object? value = CoerceType(type, property.Value);
            if (value is not null)
            {
                entity.Add(property.Key, value);
            }
        }

        return entity;
    }

    private static void ApplySystemProperty(TableEntity entity, string key, JsonNode? node)
    {
        string? value = node?.GetValue<string>();
        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        switch (key)
        {
            case "PartitionKey":
                entity.PartitionKey = value;
                break;
            case "RowKey":
                entity.RowKey = value;
                break;
            case "Timestamp":
                entity.Timestamp = DateTimeOffset.Parse(value, CultureInfo.InvariantCulture);
                break;
        }
    }

    private static object? CoerceType(string? type, JsonNode? node)
    {
        if (node is null || node.GetValueKind() == JsonValueKind.Null)
        {
            return null;
        }

        return type switch
        {
            "Boolean" => node.GetValue<bool>(),
            "DateTime" => DateTimeOffset.Parse(node.GetValue<string>(), CultureInfo.InvariantCulture),
            "Double" => node.GetValue<double>(),
            "Guid" => Guid.Parse(node.GetValue<string>()),
            "Int32" or "int" => node.GetValue<int>(),
            "Int64" or "long" => node.GetValue<long>(),
            "Binary" => Convert.FromBase64String(node.GetValue<string>()),
            _ => node.GetValue<string>(),
        };
    }
}
