# Azure.Data.Tables Extensions
Extensions for the Azure.Data.Tables library to easier access &amp; manipulate data inside Azure Table Storage

```c#
using Azure.Data.Tables;
using Medienstudio.Azure.Data.Tables.Extensions;

TableServiceClient tableServiceClient = new(connectionString);
TableClient tableClient = tableServiceClient.GetTableClient("tablename");

// Get all rows from the table
List<TableEntity> entities = await tableClient.GetAllEntitiesAsync<TableEntity>();

// Get all rows by RowKey
List<TableEntity> entities = await tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>("MyRowKey");

// Add list of entites (with auto-batching)
List<TableEntity> entities = new();
for (int i = 0; i < 1000; i++)
{
    var e = new TableEntity()
    {
        PartitionKey = (i % 20).ToString(),
        RowKey = Guid.NewGuid().ToString()
    };
    entities.Add(e);
}
await tableClient.AddEntitiesAsync(entities);

// Delete all rows from the table
await tableClient.DeleteAllEntitiesAsync();

// Create a table if it does not exists without throwing a hidden Exception that Application Insights will track
await tableServiceClient.CreateTableIfNotExistsSafeAsync(tableName);
```
