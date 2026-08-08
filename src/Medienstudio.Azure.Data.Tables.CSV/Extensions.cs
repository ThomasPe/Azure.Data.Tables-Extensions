using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using CsvHelper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Medienstudio.Azure.Data.Tables.Extensions;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Medienstudio.Azure.Data.Tables.CSV;

/// <summary>
/// Provides extension methods for exporting and importing Azure Table Storage data as CSV.
/// </summary>
public static class Extensions
{
    const string TYPE_SUFFIX = "@type";
    const string SCHEMA_ROW_KEY = "csv-export-schema";
    const string SCHEMA_JSON_PROPERTY = "SchemaJson";
    const string SOURCE_TABLE_URI_PROPERTY = "SourceTableUri";
    const int MAX_TABLE_STRING_PROPERTY_SIZE = 64 * 1024;
    static readonly string[] SYSTEM_PROPERTIES = ["PartitionKey", "RowKey", "Timestamp"];
    private static readonly ILogger NullLogger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    private static class LogEvents
    {
        public static readonly EventId CsvExportStarted = new(2000, nameof(CsvExportStarted));
        public static readonly EventId CsvExportCompleted = new(2001, nameof(CsvExportCompleted));
        public static readonly EventId CsvSchemaDiscoveryCompleted = new(2010, nameof(CsvSchemaDiscoveryCompleted));
        public static readonly EventId CsvImportStarted = new(2020, nameof(CsvImportStarted));
        public static readonly EventId CsvImportBatchSubmitting = new(2021, nameof(CsvImportBatchSubmitting));
        public static readonly EventId CsvImportBatchSubmitted = new(2022, nameof(CsvImportBatchSubmitted));
        public static readonly EventId CsvImportMissingHeader = new(2023, nameof(CsvImportMissingHeader));
        public static readonly EventId CsvImportTypeCoercionFailed = new(2024, nameof(CsvImportTypeCoercionFailed));
        public static readonly EventId CsvImportCompleted = new(2025, nameof(CsvImportCompleted));
        public static readonly EventId CsvImportInvalidColumn = new(2026, nameof(CsvImportInvalidColumn));
        public static readonly EventId CsvImportInvalidTimestamp = new(2027, nameof(CsvImportInvalidTimestamp));
        public static readonly EventId CsvStoredSchemaRetrieved = new(2030, nameof(CsvStoredSchemaRetrieved));
        public static readonly EventId CsvStoredSchemaStored = new(2031, nameof(CsvStoredSchemaStored));
    }


    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <returns>Task<void></void></returns>
    public static async Task ExportCSVAsync(this TableClient tableClient, TextWriter writer)
    {
        await ExportCSVWithLoggingAsync(tableClient, writer, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer with logging support.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>Task<void></void></returns>
    public static async Task ExportCSVWithLoggingAsync(this TableClient tableClient, TextWriter writer, ILogger? logger)
    {
        logger ??= NullLogger;
        CsvExportSchema schema = await tableClient.GetCSVExportSchemaAsync(logger).ConfigureAwait(false);
        await tableClient.ExportCSVAsync(writer, schema, logger).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the CSV export schema discovered from all entities in the table.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <returns>The non-system properties in the order they are first encountered.</returns>
    public static async Task<CsvExportSchema> GetCSVExportSchemaAsync(this TableClient tableClient)
    {
        return await GetCSVExportSchemaAsync(tableClient, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the CSV export schema discovered from all entities in the table with logging support.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>The non-system properties in the order they are first encountered.</returns>
    public static async Task<CsvExportSchema> GetCSVExportSchemaAsync(this TableClient tableClient, ILogger? logger)
    {
        logger ??= NullLogger;
        HashSet<string> knownProperties = new(StringComparer.Ordinal);
        List<string> properties = [];
        int rowsScanned = 0;

        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            rowsScanned++;
            AddProperties(row, knownProperties, properties);
        }

        logger.LogInformation(LogEvents.CsvSchemaDiscoveryCompleted, "Discovered CSV export schema for table {TableName}. Rows scanned: {RowsScanned}, properties discovered: {PropertyCount}.", tableClient.Name, rowsScanned, properties.Count);

        return new CsvExportSchema(properties);
    }

    /// <summary>
    /// Gets a previously stored CSV export schema for this table.
    /// </summary>
    /// <param name="tableClient">The source table.</param>
    /// <param name="metadataTableClient">A separate table that stores CSV export schemas.</param>
    /// <returns>The stored schema, or <see langword="null"/> when no schema has been stored.</returns>
    /// <exception cref="InvalidOperationException">The metadata table is the source table, or the stored schema is invalid.</exception>
    public static async Task<CsvExportSchema?> GetStoredCSVExportSchemaAsync(this TableClient tableClient, TableClient metadataTableClient)
    {
        return await GetStoredCSVExportSchemaAsync(tableClient, metadataTableClient, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets a previously stored CSV export schema for this table with logging support.
    /// </summary>
    /// <param name="tableClient">The source table.</param>
    /// <param name="metadataTableClient">A separate table that stores CSV export schemas.</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>The stored schema, or <see langword="null"/> when no schema has been stored.</returns>
    /// <exception cref="InvalidOperationException">The metadata table is the source table, or the stored schema is invalid.</exception>
    public static async Task<CsvExportSchema?> GetStoredCSVExportSchemaAsync(this TableClient tableClient, TableClient metadataTableClient, ILogger? logger)
    {
        logger ??= NullLogger;
        EnsureSeparateMetadataTable(tableClient, metadataTableClient);

        global::Azure.NullableResponse<TableEntity> response = await metadataTableClient.GetEntityIfExistsAsync<TableEntity>(GetSchemaPartitionKey(tableClient), SCHEMA_ROW_KEY);
        if (!response.HasValue)
        {
            logger.LogInformation(LogEvents.CsvStoredSchemaRetrieved, "No stored CSV export schema found for table {TableName}.", tableClient.Name);
            return null;
        }

        TableEntity metadata = response.Value!;
        string? schemaJson = metadata.GetString(SCHEMA_JSON_PROPERTY);
        if (string.IsNullOrEmpty(schemaJson))
        {
            throw new InvalidOperationException("The stored CSV export schema is missing its property list.");
        }

        try
        {
            string[]? properties = JsonSerializer.Deserialize<string[]>(schemaJson);
            CsvExportSchema result = properties is null
                ? throw new InvalidOperationException("The stored CSV export schema has an invalid property list.")
                : new CsvExportSchema(properties);
            logger.LogInformation(LogEvents.CsvStoredSchemaRetrieved, "Retrieved stored CSV export schema for table {TableName}. Property count: {PropertyCount}.", tableClient.Name, result.Properties.Count);
            return result;
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException("The stored CSV export schema has an invalid property list.", exception);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException("The stored CSV export schema has an invalid property list.", exception);
        }
    }

    /// <summary>
    /// Stores a CSV export schema for this table.
    /// </summary>
    /// <param name="tableClient">The source table.</param>
    /// <param name="metadataTableClient">A separate table that stores CSV export schemas.</param>
    /// <param name="schema">The schema to store.</param>
    /// <returns>A task that represents the asynchronous store operation.</returns>
    /// <exception cref="ArgumentException">The serialized schema exceeds the Azure Table Storage string property limit.</exception>
    /// <exception cref="InvalidOperationException">The metadata table is the source table.</exception>
    public static async Task StoreCSVExportSchemaAsync(this TableClient tableClient, TableClient metadataTableClient, CsvExportSchema schema)
    {
        await StoreCSVExportSchemaAsync(tableClient, metadataTableClient, schema, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Stores a CSV export schema for this table with logging support.
    /// </summary>
    /// <param name="tableClient">The source table.</param>
    /// <param name="metadataTableClient">A separate table that stores CSV export schemas.</param>
    /// <param name="schema">The schema to store.</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>A task that represents the asynchronous store operation.</returns>
    /// <exception cref="ArgumentException">The serialized schema exceeds the Azure Table Storage string property limit.</exception>
    /// <exception cref="InvalidOperationException">The metadata table is the source table.</exception>
    public static async Task StoreCSVExportSchemaAsync(this TableClient tableClient, TableClient metadataTableClient, CsvExportSchema schema, ILogger? logger)
    {
        logger ??= NullLogger;
        EnsureSeparateMetadataTable(tableClient, metadataTableClient);
        ArgumentNullException.ThrowIfNull(schema);

        string schemaJson = JsonSerializer.Serialize(schema.Properties);
        if (Encoding.UTF8.GetByteCount(schemaJson) > MAX_TABLE_STRING_PROPERTY_SIZE)
        {
            throw new ArgumentException("The serialized CSV export schema exceeds the Azure Table Storage string property limit.", nameof(schema));
        }

        TableEntity entity = new(GetSchemaPartitionKey(tableClient), SCHEMA_ROW_KEY)
        {
            [SCHEMA_JSON_PROPERTY] = schemaJson,
            [SOURCE_TABLE_URI_PROPERTY] = tableClient.Uri.AbsoluteUri
        };

        await metadataTableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        logger.LogInformation(LogEvents.CsvStoredSchemaStored, "Stored CSV export schema for table {TableName}. Property count: {PropertyCount}.", tableClient.Name, schema.Properties.Count);
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
        await ExportCSVInMemoryAsync(tableClient, writer, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer after buffering them in memory with logging support.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>Task<void></void></returns>
    /// <remarks>Use this method only when the table fits comfortably in memory. It queries the table once and buffers every entity before writing.</remarks>
    public static async Task ExportCSVInMemoryAsync(this TableClient tableClient, TextWriter writer, ILogger? logger)
    {
        logger ??= NullLogger;
        Stopwatch stopwatch = Stopwatch.StartNew();
        List<TableEntity> rows = [];
        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            rows.Add(row);
        }

        CsvExportSchema schema = CreateCSVExportSchema(rows);
        WriteCSV(writer, schema, rows);
        stopwatch.Stop();
        logger.LogInformation(LogEvents.CsvExportCompleted, "Completed in-memory CSV export for table {TableName}. Rows exported: {RowCount}, schema properties: {PropertyCount}, durationMs: {DurationMs}.", tableClient.Name, rows.Count, schema.Properties.Count, stopwatch.ElapsedMilliseconds);
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
        await ExportCSVAsync(tableClient, writer, schema, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns all rows in the table as CSV to the provided writer using the supplied schema and logging support.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="writer">TextWriter instance that takes the serialized result</param>
    /// <param name="schema">The non-system properties to include in the CSV export.</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>Task<void></void></returns>
    /// <exception cref="InvalidOperationException">An entity contains a property that is absent from <paramref name="schema"/>.</exception>
    public static async Task ExportCSVAsync(this TableClient tableClient, TextWriter writer, CsvExportSchema schema, ILogger? logger)
    {
        logger ??= NullLogger;
        ArgumentNullException.ThrowIfNull(schema);
        Stopwatch stopwatch = Stopwatch.StartNew();
        logger.LogInformation(LogEvents.CsvExportStarted, "Starting CSV export for table {TableName}. Schema properties: {PropertyCount}.", tableClient.Name, schema.Properties.Count);

        List<string> keys = CreateCSVKeys(schema);
        HashSet<string> schemaProperties = schema.Properties.ToHashSet(StringComparer.Ordinal);
        int rowsWritten = 0;

        using CsvWriter csv = CreateCSVWriter(writer);
        WriteCSVHeader(csv, keys);

        await foreach (TableEntity row in tableClient.QueryAsync<TableEntity>())
        {
            WriteCSVRow(csv, keys, schemaProperties, row);
            rowsWritten++;
        }

        csv.Flush();
        stopwatch.Stop();
        logger.LogInformation(LogEvents.CsvExportCompleted, "Completed CSV export for table {TableName}. Rows exported: {RowCount}, schema properties: {PropertyCount}, durationMs: {DurationMs}.", tableClient.Name, rowsWritten, schema.Properties.Count, stopwatch.ElapsedMilliseconds);
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

    private static void EnsureSeparateMetadataTable(TableClient tableClient, TableClient metadataTableClient)
    {
        ArgumentNullException.ThrowIfNull(metadataTableClient);
        if (tableClient.Uri == metadataTableClient.Uri)
        {
            throw new InvalidOperationException("The metadata table must be separate from the source table.");
        }
    }

    private static string GetSchemaPartitionKey(TableClient tableClient)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(tableClient.Uri.AbsoluteUri));
        return Convert.ToHexString(hash);
    }


    /// <summary>
    /// Imports a CSV read stream to Table Storage
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="reader">TextReader instance providing access to the CSV</param>
    /// <returns></returns>
    public static async Task ImportCSVAsync(this TableClient tableClient, TextReader reader)
    {
        await ImportCSVAsync(tableClient, reader, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Imports a CSV read stream to Table Storage with logging support.
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="reader">TextReader instance providing access to the CSV</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns></returns>
    public static async Task ImportCSVAsync(this TableClient tableClient, TextReader reader, ILogger? logger)
    {
        logger ??= NullLogger;
        Stopwatch stopwatch = Stopwatch.StartNew();
        logger.LogInformation(LogEvents.CsvImportStarted, "Starting CSV import for table {TableName}.", tableClient.Name);

        using CsvReader csv = new(reader, CultureInfo.InvariantCulture);
        csv.Read();
        csv.ReadHeader();
        List<TableEntity> entities = [];
        int batchCounter = 0;
        int rowIndex = 1;
        int importedCount = 0;

        while (csv.Read())
        {
            rowIndex++;
            batchCounter++;
            // loop through fields while index is not out of bounds
            TableEntity entity = [];
            int i = 0;
            while (csv.TryGetField(i, out string? field))
            {
                if (csv.HeaderRecord is null || i >= csv.HeaderRecord.Length || string.IsNullOrEmpty(csv.HeaderRecord[i]))
                {
                    logger.LogWarning(LogEvents.CsvImportMissingHeader, "CSV import failed for table {TableName}: missing header at row {RowIndex}, column index {ColumnIndex}.", tableClient.Name, rowIndex, i);
                    throw new InvalidDataException($"CSV import failed because the header for column index {i} is missing at row {rowIndex}.");
                }
                string label = csv.HeaderRecord[i];

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
                            try
                            {
                                entity.Timestamp = DateTimeOffset.Parse(field, CultureInfo.InvariantCulture);
                            }
                            catch (FormatException ex)
                            {
                                logger.LogWarning(LogEvents.CsvImportInvalidTimestamp, ex, "CSV import failed for table {TableName}: invalid Timestamp value at row {RowIndex}.", tableClient.Name, rowIndex);
                                throw new InvalidDataException($"CSV import failed at row {rowIndex}, column 'Timestamp'.", ex);
                            }
                            break;
                    }
                }
                else if (!label.EndsWith(TYPE_SUFFIX))
                {
                    string? type = csv.GetField<string>(label + "@type")?.Split('@')[0];
                    if (!string.IsNullOrEmpty(field) || type is "String" or "Binary")
                    {
                        object value;
                        try
                        {
                            value = CoerceType(type, field ?? string.Empty);
                        }
                        catch (Exception ex) when (ex is FormatException or InvalidOperationException or OverflowException)
                        {
                            logger.LogWarning(LogEvents.CsvImportTypeCoercionFailed, ex, "CSV import failed for table {TableName}: type coercion error at row {RowIndex}, column {ColumnName}, declared type {DeclaredType}.", tableClient.Name, rowIndex, label, type ?? "String");
                            throw new InvalidDataException($"CSV import failed at row {rowIndex}, column '{label}', declared type '{type ?? "String"}'.", ex);
                        }

                        try
                        {
                            entity.Add(label, value);
                        }
                        catch (ArgumentException ex)
                        {
                            logger.LogWarning(LogEvents.CsvImportInvalidColumn, ex, "CSV import failed for table {TableName}: invalid column {ColumnName} at row {RowIndex}.", tableClient.Name, label, rowIndex);
                            throw new InvalidDataException($"CSV import failed at row {rowIndex}, column '{label}'.", ex);
                        }
                    }
                }

                i++;
            }
            entities.Add(entity);

            if (batchCounter == 100)
            {
                logger.LogDebug(LogEvents.CsvImportBatchSubmitting, "Submitting CSV import batch for table {TableName}. Batch entity count: {BatchEntityCount}.", tableClient.Name, entities.Count);
                await SubmitImportBatchAsync(tableClient, entities).ConfigureAwait(false);
                logger.LogDebug(LogEvents.CsvImportBatchSubmitted, "Submitted CSV import batch for table {TableName}. Batch entity count: {BatchEntityCount}.", tableClient.Name, entities.Count);
                importedCount += entities.Count;
                entities = [];
                batchCounter = 0;
            }
        }
        if (entities.Count > 0)
        {
            logger.LogDebug(LogEvents.CsvImportBatchSubmitting, "Submitting final CSV import batch for table {TableName}. Batch entity count: {BatchEntityCount}.", tableClient.Name, entities.Count);
            await SubmitImportBatchAsync(tableClient, entities).ConfigureAwait(false);
            logger.LogDebug(LogEvents.CsvImportBatchSubmitted, "Submitted final CSV import batch for table {TableName}. Batch entity count: {BatchEntityCount}.", tableClient.Name, entities.Count);
            importedCount += entities.Count;
        }
        stopwatch.Stop();
        logger.LogInformation(LogEvents.CsvImportCompleted, "Completed CSV import for table {TableName}. Rows imported: {ImportedCount}, durationMs: {DurationMs}.", tableClient.Name, importedCount, stopwatch.ElapsedMilliseconds);
    }

    private static async Task SubmitImportBatchAsync(TableClient tableClient, List<TableEntity> entities)
    {
        foreach (IGrouping<string, TableEntity> partition in entities.GroupBy(entity => entity.PartitionKey))
        {
            List<TableTransactionAction> actions = [.. partition.Select(entity => new TableTransactionAction(TableTransactionActionType.Add, entity))];
            await tableClient.SubmitTransactionAsync(actions).ConfigureAwait(false);
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
