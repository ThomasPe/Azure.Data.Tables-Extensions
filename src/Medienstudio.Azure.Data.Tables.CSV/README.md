# Azure.Data.Tables Extensions
Extensions for the Azure.Data.Tables library to easier access &amp; manipulate data inside Azure Table Storage.

With the CSV package you can easily import and export data from Azure Table Storage to CSV files.

## NOTE
If you use this code for backups, please test both export and import functionality and verify that the data is correct. I am not responsible for any data loss.

**A backup that you didn't test is not a backup.**

## NuGet
[![Nuget](https://img.shields.io/nuget/v/Medienstudio.Azure.Data.Tables.Extensions?label=Medienstudio.Azure.Data.Tables.Extensions%20on%20NuGet)](https://www.nuget.org/packages/Medienstudio.Azure.Data.Tables.Extensions/)

[![Nuget](https://img.shields.io/nuget/v/Medienstudio.Azure.Data.Tables.CSV?label=Medienstudio.Azure.Data.Tables.CSV%20on%20NuGet)](https://www.nuget.org/packages/Medienstudio.Azure.Data.Tables.CSV/)

## CSV Export / Import

The CSV package aims support Azure Table Storage data import & export support for CSV exactly as the Azure Storage Explorer and the old Azure CLI v7 does it. Therefore exported files should be importable by the Azure Storage Explorer and the package can also read exports from ASE. 

```csharp
using Azure.Data.Tables;
using Medienstudio.Azure.Data.Tables.CSV;

TableServiceClient tableServiceClient = new(connectionString);
TableClient tableClient = tableServiceClient.GetTableClient("tablename");

// Export all rows from the table to a CSV file
CreateTestData();
using StreamWriter writer = File.CreateText("test.csv");
await _tableClient.ExportCSVAsync(writer);

// Reuse a discovered schema to stream subsequent exports in a single table query
CsvExportSchema schema = await _tableClient.GetCSVExportSchemaAsync();
using StreamWriter schemaWriter = File.CreateText("test-with-schema.csv");
await _tableClient.ExportCSVAsync(schemaWriter, schema);

// Query the table once and buffer all rows before exporting
using StreamWriter inMemoryWriter = File.CreateText("test-in-memory.csv");
await _tableClient.ExportCSVInMemoryAsync(inMemoryWriter);

// Export all rows as CSV to Azure BLob Storage
BlobContainerClient containerClient = new(BlobConnectionString, "testcontainer");
var blobClient = containerClient.GetBlobClient("test.csv");
var stream = await blobClient.OpenWriteAsync(true, new BlobOpenWriteOptions() { HttpHeaders = new BlobHttpHeaders { ContentType = "text/csv" } });
using StreamWriter writer = new(stream);
await _tableClient.ExportCSVAsync(writer);

// Import all rows from a CSV file to the table
using StreamReader reader = new("test.csv");
await _tableClient.ImportCSVAsync(reader);
```

`ExportCSVAsync(writer)` discovers the table properties before streaming rows, so it queries the table twice. Reuse a `CsvExportSchema` for later one-query exports. If an entity contains a property outside the supplied schema, the export fails instead of omitting that column. A schema contains only custom table properties; system properties and `odata.etag` cannot be supplied. `ExportCSVInMemoryAsync(writer)` queries the table once, but buffers every entity and should only be used when the table fits comfortably in memory.