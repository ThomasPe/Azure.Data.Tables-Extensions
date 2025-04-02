using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;

namespace Medienstudio.Azure.Data.Tables.Extensions;

/// <summary>
/// Class providing Azure Data Table Extension methods
/// </summary>
public static class Extensions
{
    /// <summary>
    /// Groups entities by PartitionKey into batches of max 100 for valid transactions
    /// </summary>
    /// <returns>List of Azure Responses for Transactions</returns>
    public static async Task<List<Response<IReadOnlyList<Response>>>> BatchManipulateEntities<T>(TableClient tableClient, IEnumerable<T> entities, TableTransactionActionType tableTransactionActionType) where T : class, ITableEntity, new()
    {
        IEnumerable<IGrouping<string, T>> groups = entities.GroupBy(x => x.PartitionKey);
        List<Response<IReadOnlyList<Response>>> responses = [];
        foreach (IGrouping<string, T> group in groups)
        {
            List<TableTransactionAction> actions;
            IEnumerable<T> items = group.AsEnumerable();
            while (items.Any())
            {
                IEnumerable<T> batch = items.Take(100);
                items = items.Skip(100);

                actions = [.. batch.Select(e => new TableTransactionAction(tableTransactionActionType, e))];
                Response<IReadOnlyList<Response>> response = await tableClient.SubmitTransactionAsync(actions).ConfigureAwait(false);
                responses.Add(response);
            }
        }
        return responses;
    }

    /// <summary>
    /// Returns all rows in a given Partition
    /// </summary>
    /// <typeparam name="T">Implementation of ITableEntity</typeparam>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="partitionKey">The PartitionKey</param>
    /// <returns>List of all entities with specified PartitionKey</returns>
    public static async Task<IList<T>> GetAllEntitiesByPartitionKeyAsync<T>(this TableClient tableClient, string partitionKey) where T : class, ITableEntity, new()
    {
        return await tableClient.QueryAsync<T>(x => x.PartitionKey == partitionKey, maxPerPage: 1000).ToListAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns all entities where values in specified column start with specified prefix
    /// </summary>
    /// <typeparam name="T">Implementation of ITableEntity</typeparam>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="column">Column name on which to filter</param>
    /// <param name="prefix">String with which the column value should start with</param>
    /// <returns></returns>
    public static async Task<IList<T>> GetAllEntitiesStartingWithAsync<T>(this TableClient tableClient, string column, string prefix) where T : class, ITableEntity, new()
    {
        return await tableClient.QueryAsync<T>(Helpers.StartsWith(column, prefix), maxPerPage: 1000).ToListAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns all rows for a given RowKey
    /// </summary>
    /// <typeparam name="T">Implementation of ITableEntity</typeparam>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="rowKey">The RowKey</param>
    /// <remarks>Will result in a Table Scan which can result in bad query performance</remarks>
    /// <returns>List of all entities in the table with specified RowKey</returns>
    public static async Task<IList<T>> GetAllEntitiesByRowKeyAsync<T>(this TableClient tableClient, string rowKey) where T : class, ITableEntity, new()
    {
        return await tableClient.QueryAsync<T>(x => x.RowKey == rowKey, maxPerPage: 1000).ToListAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns all rows in the table
    /// </summary>
    /// <typeparam name="T">Implementation of ITableEntity</typeparam>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <returns></returns>
    public static async Task<List<T>> GetAllEntitiesAsync<T>(this TableClient tableClient) where T : class, ITableEntity, new()
    {
        return await tableClient.QueryAsync<T>(maxPerPage: 1000).ToListAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns first entity in the table
    /// </summary>
    /// <typeparam name="T">Implementation of ITableEntity</typeparam>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <returns>First entity in table</returns>
    public static async Task<T?> GetFirstEntityAsync<T>(this TableClient tableClient) where T : class, ITableEntity, new()
    {
        return await tableClient.QueryAsync<T>(maxPerPage: 1).FirstOrDefaultAsync();
    }

    /// <summary>
    /// Returns first entity of the partition
    /// </summary>
    /// <typeparam name="T">Implementation of ITableEntity</typeparam>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="partitionKey">PartitionKey</param>
    /// <returns>First entity in partition</returns>
    public static async Task<T?> GetFirstEntityAsync<T>(this TableClient tableClient, string partitionKey) where T : class, ITableEntity, new()
    {
        return await tableClient.QueryAsync<T>(filter: x => x.PartitionKey == partitionKey, maxPerPage: 1).FirstOrDefaultAsync();
    }

    /// <summary>
    /// Add a list of entites with automatic batching by PartitionKey
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableClient"></param>
    /// <param name="entities"></param>
    /// <param name="tableTransactionActionType"></param>
    /// <returns></returns>
    public static async Task AddEntitiesAsync<T>(this TableClient tableClient, IEnumerable<T> entities, TableTransactionActionType tableTransactionActionType = TableTransactionActionType.Add) where T : class, ITableEntity, new()
    {
        await BatchManipulateEntities(tableClient, entities, tableTransactionActionType).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes all rows from the table
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <returns></returns>
    public static async Task DeleteAllEntitiesAsync(this TableClient tableClient)
    {
        // Only the PartitionKey & RowKey fields are required for deletion
        AsyncPageable<TableEntity> entities = tableClient
            .QueryAsync<TableEntity>(select: ["PartitionKey", "RowKey"], maxPerPage: 1000);

        await entities.AsPages().ForEachAwaitAsync(async page =>
        {
            // Since we don't know how many rows the table has and the results are ordered by PartitonKey+RowKey
            // we'll delete each page immediately and not cache the whole table in memory
            await BatchManipulateEntities(tableClient, page.Values, TableTransactionActionType.Delete).ConfigureAwait(false);
        });
    }

    /// <summary>
    /// Deletes all rows with the given PartitionKey
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="partitionKey">The PartitionKey</param>
    /// <returns></returns>
    public static async Task DeleteAllEntitiesByPartitionKeyAsync(this TableClient tableClient, string partitionKey)
    {
        // Only the PartitionKey & RowKey fields are required for deletion
        AsyncPageable<TableEntity> entities = tableClient
            .QueryAsync<TableEntity>(x => x.PartitionKey == partitionKey, select: ["PartitionKey", "RowKey"], maxPerPage: 1000);

        await entities.AsPages().ForEachAwaitAsync(async page =>
        {
            // Since we don't know how many rows the table has and the results are ordered by PartitonKey+RowKey
            // we'll delete each page immediately and not cache the whole table in memory
            await BatchManipulateEntities(tableClient, page.Values, TableTransactionActionType.Delete).ConfigureAwait(false);
        });
    }

    /// <summary>
    /// Creates a table without throwing a hidden expcetion when it already exists
    /// </summary>
    /// <param name="tableServiceClient">Authenticated TableServiceClient</param>
    /// <param name="table">The table name</param>
    /// <returns>Azure Response, null if table already existed</returns>
    public static async Task<Response<TableItem>?> CreateTableIfNotExistsSafeAsync(this TableServiceClient tableServiceClient, string table)
    {
        List<TableItem> tables = await tableServiceClient.QueryAsync(x => x.Name == table).ToListAsync().ConfigureAwait(false);
        if (tables.Count == 0)
        {
            return await tableServiceClient.CreateTableAsync(table).ConfigureAwait(false);
        }
        return null;
    }

    /// <summary>
    /// Synchronously creates a table without throwing a hidden expcetion when it already exists
    /// </summary>
    /// <param name="tableServiceClient">Authenticated TableServiceClient</param>
    /// <param name="table">The table name</param>
    /// <returns>Azure Response, null if table already existed</returns>
    public static Response<TableItem>? CreateTableIfNotExistsSafe(this TableServiceClient tableServiceClient, string table)
    {
        List<TableItem> tables = tableServiceClient.Query(x => x.Name == table).ToList();
        if (tables.Count == 0)
        {
            return tableServiceClient.CreateTable(table);
        }
        return null;
    }

    /// <summary>
    /// Counts all rows in the table
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="partitionKey">The PartitionKey to filter by, or null to count all rows</param>
    /// <returns>The total number of rows in the table</returns>
    public static async Task<int> CountEntitiesAsync(this TableClient tableClient, string? partitionKey = null)
    {
        string? filter = partitionKey is null ? null : TableClient.CreateQueryFilter($"PartitionKey eq '{partitionKey}'");
        IAsyncEnumerable<Page<TableEntity>> pages = tableClient
            .QueryAsync<TableEntity>(filter: filter, select: [], maxPerPage: 1000)
            .AsPages();

        int count = 0;
        await foreach (Page<TableEntity> page in pages)
        {
            count += page.Values.Count;
        }
        return count;
    }
}
