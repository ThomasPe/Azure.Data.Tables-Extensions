using Azure.Data.Tables;
using CsvHelper;
using Medienstudio.Azure.Data.Tables.Extensions;
using System.Globalization;

namespace Medienstudio.Azure.Data.Tables.CSV;

public static class Extensions
{
    const string TYPE_SUFFIX = "@type";
    static readonly string[] SYSTEM_PROPERTIES = { "PartitionKey", "RowKey", "Timestamp" };


    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <returns>Task<void></void></returns>
    public static async Task ExportCSVAsync(this TableClient tableClient, TextWriter writer)
    {
        List<TableEntity> rows = new(0);
        List<string> systemProperties = new(3) { "PartitionKey", "RowKey", "Timestamp" };
        List<string> keys = new(3);
        keys.AddRange(systemProperties);
        List<string> ignore = new(1) { "odata.etag" };

        using CsvWriter csv = new(writer, CultureInfo.InvariantCulture);

        // preserve milliseconds, truncate trailing zeros
        csv.Context.TypeConverterOptionsCache.GetOptions<DateTime>().Formats = ["yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"];
        csv.Context.TypeConverterOptionsCache.GetOptions<DateTimeOffset>().Formats = ["yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"];


        // serialize byte arrays as base64 strings
        csv.Context.TypeConverterCache.AddConverter<byte[]>(new BinaryConverter());

        // serilaize booleans lowercase
        csv.Context.TypeConverterCache.AddConverter<bool>(new BoolConverter());


        IAsyncEnumerable<global::Azure.Page<TableEntity>> query = tableClient.QueryAsync<TableEntity>().AsPages();

        // loop through all result pages
        await foreach (global::Azure.Page<TableEntity> page in query)
        {
            // loop through all rows in the page
            foreach (TableEntity entity in page.Values)
            {
                // cache all rows
                rows.Add(entity);

                // prepare the list of keys for csv header
                foreach (KeyValuePair<string, object> property in entity)
                {
                    // skip properties that should be ignored for the export like odata.etag
                    if (ignore.Contains(property.Key))
                    {
                        continue;
                    }
                    // add the property key to the list of keys if it is not already in the list
                    if (!keys.Contains(property.Key))
                    {
                        keys.Add(property.Key);
                    }
                    // add the type of the property to the list of keys if it is not a system property
                    if (!systemProperties.Contains(property.Key) && !keys.Contains(property.Key + "@type"))
                    {
                        keys.Add(property.Key + "@type");
                    }
                }
            }
        }

        // create the csv header
        foreach (string key in keys)
        {
            csv.WriteField(key);
        }
        csv.NextRecord();

        // create the csv rows
        foreach (TableEntity row in rows)
        {
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
                    if (row.ContainsKey(key.Substring(0, key.Length - TYPE_SUFFIX.Length)))
                    {
                        csv.WriteField(row[key.Substring(0, key.Length - TYPE_SUFFIX.Length)].GetPropertyTypeName());
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

        csv.Flush();
    }


    /// <summary>
    /// Imports a CSV read stream to Table Storage
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="reader">TextReader instance providing access to the CSV</param>
    /// <returns></returns>
    public static async Task ImportCSVAsync(this TableClient tableClient, TextReader reader)
    {
        using CsvReader csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        csv.Read();
        csv.ReadHeader();
        List<TableEntity> entities = new(0);
        int batchCounter = 0;

        while (csv.Read())
        {
            batchCounter++;
            // loop through fields while index is not out of bounds
            TableEntity entity = new();
            int i = 0;
            while (csv.TryGetField(i, out string? field))
            {
                if (string.IsNullOrEmpty(field))
                {
                    i++;
                    continue;
                }
                string? label = csv.HeaderRecord?[i];
                if (label == null)
                {
                    return;
                }

                if (SYSTEM_PROPERTIES.Contains(label))
                {
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
                    object value = CoerceType(type, field);
                    entity.Add(label, value);
                }

                i++;
            }
            entities.Add(entity);

            if (batchCounter == 100)
            {
                await tableClient.AddEntitiesAsync(entities);
                entities = new();
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
