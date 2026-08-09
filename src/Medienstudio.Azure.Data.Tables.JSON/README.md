# Azure.Data.Tables Extensions
Extensions for the Azure.Data.Tables library to easier access &amp; manipulate data inside Azure Table Storage.

With the JSON package you can easily import and export data from Azure Table Storage as newline-delimited JSON (NDJSON).

## NOTE
If you use this code for backups, please test both export and import functionality and verify that the data is correct. I am not responsible for any data loss.

**A backup that you didn't test is not a backup.**

## NuGet
[![Nuget](https://img.shields.io/nuget/v/Medienstudio.Azure.Data.Tables.Extensions?label=Medienstudio.Azure.Data.Tables.Extensions%20on%20NuGet)](https://www.nuget.org/packages/Medienstudio.Azure.Data.Tables.Extensions/)

[![Nuget](https://img.shields.io/nuget/v/Medienstudio.Azure.Data.Tables.JSON?label=Medienstudio.Azure.Data.Tables.JSON%20on%20NuGet)](https://www.nuget.org/packages/Medienstudio.Azure.Data.Tables.JSON/)

## JSON Export / Import

Each row is written as a single self-describing JSON object, one per line (NDJSON). Unlike CSV, a fixed column list is not required up front: every row simply serializes the properties it has, so tables with heterogeneous entities export without a schema-discovery pass. `Int32`, `Int64`, and `Double` properties are written as native JSON numbers, and `Boolean` as a native JSON boolean. Property types JSON cannot represent unambiguously (`DateTime`, `Guid`, `Binary`) are written as strings. Every property is written alongside a sibling `PropertyName@type` field carrying its .NET type name, mirroring the CSV package's convention, so the importer can restore the exact original type without guessing from the JSON value's shape.

```csharp
using Azure.Data.Tables;
using Medienstudio.Azure.Data.Tables.JSON;

TableServiceClient tableServiceClient = new(connectionString);
TableClient tableClient = tableServiceClient.GetTableClient("tablename");

// Export all rows from the table to an NDJSON file
using StreamWriter writer = File.CreateText("test.ndjson");
await tableClient.ExportJSONAsync(writer);

// Query the table once and buffer all rows before exporting
using StreamWriter inMemoryWriter = File.CreateText("test-in-memory.ndjson");
await tableClient.ExportJSONInMemoryAsync(inMemoryWriter);

// Export all rows as NDJSON to Azure Blob Storage
BlobContainerClient containerClient = new(BlobConnectionString, "testcontainer");
BlobClient blobClient = containerClient.GetBlobClient("test.ndjson");
Stream blobStream = await blobClient.OpenWriteAsync(true, new BlobOpenWriteOptions() { HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-ndjson" } });
using StreamWriter blobWriter = new(blobStream);
await tableClient.ExportJSONAsync(blobWriter);

// Import all rows from an NDJSON file to the table
using StreamReader reader = new("test.ndjson");
await tableClient.ImportJSONAsync(reader);
```

`ExportJSONAsync(writer)` streams rows as it queries the table, so it never buffers more than one entity at a time. `ExportJSONInMemoryAsync(writer)` queries the table once but buffers every entity before writing and should only be used when the table fits comfortably in memory.
