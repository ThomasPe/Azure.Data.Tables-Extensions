# Azure.Data.Tables Extensions
Extensions for the Azure.Data.Tables library to easier access &amp; manipulate data inside Azure Table Storage.

With the CSV package you can easily import and export data from Azure Table Storage to CSV files.

## NOTE
If you use this code for backups, please test both export and import functionality and verify that the data is correct. I am not responsible for any data loss.

**A backup that you didn't test is not a backup.**

## NuGet
[![Nuget](https://img.shields.io/nuget/v/Medienstudio.Azure.Data.Tables.Extensions?label=Medienstudio.Azure.Data.Tables.Extensions%20on%20NuGet)](https://www.nuget.org/packages/Medienstudio.Azure.Data.Tables.Extensions/)

[![Nuget](https://img.shields.io/nuget/v/Medienstudio.Azure.Data.Tables.CSV?label=Medienstudio.Azure.Data.Tables.CSV%20on%20NuGet)](https://www.nuget.org/packages/Medienstudio.Azure.Data.Tables.CSV/)

## Querying

```csharp
using Azure.Data.Tables;
using Medienstudio.Azure.Data.Tables.Extensions;

TableServiceClient tableServiceClient = new(connectionString);
TableClient tableClient = tableServiceClient.GetTableClient("tablename");

// Get all rows from the table
List<TableEntity> entities = await tableClient.GetAllEntitiesAsync<TableEntity>();  

// Get all rows by PartitionKey
List<TableEntity> entities = await tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("MyPartitionKey");

// Get all rows by RowKey
List<TableEntity> entities = await tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>("MyRowKey");

// Get all entites where a column starts with a specific value
List<TableEntity> entities = await _tableClient.GetAllEntitiesStartingWithAsync<TableEntity>("MyColumn", "abc");

// Get first entity in table (smallest PartitionKey + RowKey)
TableEntity entity = await tableClient.GetFirstEntityAsync<TableEntity>();

// Add list of entites (with auto-batching)
List<TableEntity> entities = new(){...};
await tableClient.AddEntitiesAsync(entities);

// Delete all rows from the table
await tableClient.DeleteAllEntitiesAsync();

// Delete all rows by PartitionKey
await _tableClient.DeleteAllEntitiesByPartitionKeyAsync("123");

// Create a table if it does not exists without throwing a hidden Exception that Application Insights will track
await tableServiceClient.CreateTableIfNotExistsSafeAsync(tableName);
```

## CSV Export / Import

The CSV package aims support Azure Table Storage data import & export support for CSV exactly as the Azure Storage Explorer and the old Azure CLI v7 does it. Therefore exported files should be importable by the Azure Storage Explorer and the package can also read exports from ASE. 

```csharp
using Azure.Data.Tables;
using Medienstudio.Azure.Data.Tables.CSV;

TableServiceClient tableServiceClient = new(connectionString);
TableClient tableClient = tableServiceClient.GetTableClient("tablename");

// Export all rows from the table to a CSV file
using StreamWriter writer = File.CreateText("test.csv");
await _tableClient.ExportCSVAsync(writer);

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
