using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Medienstudio.Azure.Data.Tables.Extensions;

/// <summary>
/// Class providing Azure Data Table Extension methods
/// </summary>
public static class Extensions
{
    private static readonly ILogger NullLogger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    private static class LogEvents
    {
        public static readonly EventId BatchManipulateStarted = new(1000, nameof(BatchManipulateStarted));
        public static readonly EventId BatchSubmitted = new(1001, nameof(BatchSubmitted));
        public static readonly EventId BatchManipulateCompleted = new(1002, nameof(BatchManipulateCompleted));
        public static readonly EventId AddEntitiesStarted = new(1010, nameof(AddEntitiesStarted));
        public static readonly EventId AddEntitiesCompleted = new(1011, nameof(AddEntitiesCompleted));
        public static readonly EventId DeleteAllEntitiesStarted = new(1020, nameof(DeleteAllEntitiesStarted));
        public static readonly EventId DeleteAllEntitiesPage = new(1021, nameof(DeleteAllEntitiesPage));
        public static readonly EventId DeleteAllEntitiesCompleted = new(1022, nameof(DeleteAllEntitiesCompleted));
        public static readonly EventId DeletePartitionStarted = new(1030, nameof(DeletePartitionStarted));
        public static readonly EventId DeletePartitionPage = new(1031, nameof(DeletePartitionPage));
        public static readonly EventId DeletePartitionCompleted = new(1032, nameof(DeletePartitionCompleted));
    }

    /// <summary>
    /// Groups entities by PartitionKey into batches of max 100 for valid transactions
    /// </summary>
    /// <returns>List of Azure Responses for Transactions</returns>
    public static async Task<List<Response<IReadOnlyList<Response>>>> BatchManipulateEntities<T>(TableClient tableClient, IEnumerable<T> entities, TableTransactionActionType tableTransactionActionType) where T : class, ITableEntity, new()
    {
        return await BatchManipulateEntities(tableClient, entities, tableTransactionActionType, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Groups entities by PartitionKey into batches of max 100 for valid transactions with logging support.
    /// </summary>
    /// <returns>List of Azure Responses for Transactions</returns>
    public static async Task<List<Response<IReadOnlyList<Response>>>> BatchManipulateEntities<T>(TableClient tableClient, IEnumerable<T> entities, TableTransactionActionType tableTransactionActionType, ILogger? logger) where T : class, ITableEntity, new()
    {
        logger ??= NullLogger;
        IEnumerable<IGrouping<string, T>> groups = entities.GroupBy(x => x.PartitionKey);
        List<Response<IReadOnlyList<Response>>> responses = [];
        int partitionCount = 0;
        int submittedBatchCount = 0;
        int submittedEntityCount = 0;

        logger.LogInformation(LogEvents.BatchManipulateStarted, "Starting batched table transaction with action {ActionType}.", tableTransactionActionType);
        foreach (IGrouping<string, T> group in groups)
        {
            partitionCount++;
            List<TableTransactionAction> actions;
            IEnumerable<T> items = group.AsEnumerable();
            while (items.Any())
            {
                IEnumerable<T> batch = items.Take(100);
                items = items.Skip(100);

                actions = [.. batch.Select(e => new TableTransactionAction(tableTransactionActionType, e))];
                int batchCount = actions.Count;
                logger.LogDebug(LogEvents.BatchSubmitted, "Submitting table transaction batch for partition {PartitionKey} containing {BatchEntityCount} entities.", group.Key, batchCount);
                Response<IReadOnlyList<Response>> response = await tableClient.SubmitTransactionAsync(actions).ConfigureAwait(false);
                responses.Add(response);
                submittedBatchCount++;
                submittedEntityCount += batchCount;
            }
        }
        logger.LogInformation(LogEvents.BatchManipulateCompleted, "Completed batched table transactions with action {ActionType}. Partitions: {PartitionCount}, batches: {BatchCount}, entities: {EntityCount}.", tableTransactionActionType, partitionCount, submittedBatchCount, submittedEntityCount);
        return responses;
    }

    /// <summary>
    /// Returns all rows in a given Partition
    /// </summary>
    /// <typeparam name="T">Implementation of ITableEntity</typeparam>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="partitionKey">The PartitionKey</param>
    /// <returns>List of all entities with specified PartitionKey</returns>
    public static async Task<List<T>> GetAllEntitiesByPartitionKeyAsync<T>(this TableClient tableClient, string partitionKey) where T : class, ITableEntity, new()
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
    public static async Task<List<T>> GetAllEntitiesStartingWithAsync<T>(this TableClient tableClient, string column, string prefix) where T : class, ITableEntity, new()
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
    public static async Task<List<T>> GetAllEntitiesByRowKeyAsync<T>(this TableClient tableClient, string rowKey) where T : class, ITableEntity, new()
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
        await AddEntitiesAsync(tableClient, entities, tableTransactionActionType, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Add a list of entities with automatic batching by PartitionKey.
    /// </summary>
    public static async Task AddEntitiesAsync<T>(this TableClient tableClient, IEnumerable<T> entities, TableTransactionActionType tableTransactionActionType, ILogger? logger) where T : class, ITableEntity, new()
    {
        logger ??= NullLogger;
        List<T> entityList = [.. entities];
        int entityCount = entityList.Count;
        logger.LogInformation(LogEvents.AddEntitiesStarted, "Adding entities with action {ActionType}. Total entities: {EntityCount}.", tableTransactionActionType, entityCount);
        await BatchManipulateEntities(tableClient, entityList, tableTransactionActionType, logger).ConfigureAwait(false);
        logger.LogInformation(LogEvents.AddEntitiesCompleted, "Completed adding entities with action {ActionType}. Total entities: {EntityCount}.", tableTransactionActionType, entityCount);
    }

    /// <summary>
    /// Deletes all rows from the table
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <returns></returns>
    public static async Task DeleteAllEntitiesAsync(this TableClient tableClient)
    {
        await DeleteAllEntitiesAsync(tableClient, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes all rows from the table with logging support.
    /// </summary>
    public static async Task DeleteAllEntitiesAsync(this TableClient tableClient, ILogger? logger)
    {
        logger ??= NullLogger;
        logger.LogInformation(LogEvents.DeleteAllEntitiesStarted, "Deleting all entities from table {TableName}.", tableClient.Name);

        // Only the PartitionKey & RowKey fields are required for deletion
        AsyncPageable<TableEntity> entities = tableClient
            .QueryAsync<TableEntity>(select: ["PartitionKey", "RowKey"], maxPerPage: 1000);

        int totalDeleted = 0;
        await foreach (Page<TableEntity> page in entities.AsPages())
        {
            // Since we don't know how many rows the table has and the results are ordered by PartitonKey+RowKey
            // we'll delete each page immediately and not cache the whole table in memory
            logger.LogDebug(LogEvents.DeleteAllEntitiesPage, "Deleting page with {EntityCount} entities from table {TableName}.", page.Values.Count, tableClient.Name);
            await BatchManipulateEntities(tableClient, page.Values, TableTransactionActionType.Delete, logger).ConfigureAwait(false);
            totalDeleted += page.Values.Count;
        }
        logger.LogInformation(LogEvents.DeleteAllEntitiesCompleted, "Completed deleting all entities from table {TableName}. Deleted entities: {DeletedEntityCount}.", tableClient.Name, totalDeleted);
    }

    /// <summary>
    /// Deletes all rows with the given PartitionKey
    /// </summary>
    /// <param name="tableClient">The authenticated TableClient</param>
    /// <param name="partitionKey">The PartitionKey</param>
    /// <returns></returns>
    public static async Task DeleteAllEntitiesByPartitionKeyAsync(this TableClient tableClient, string partitionKey)
    {
        await DeleteAllEntitiesByPartitionKeyAsync(tableClient, partitionKey, NullLogger).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes all rows with the given PartitionKey with logging support.
    /// </summary>
    public static async Task DeleteAllEntitiesByPartitionKeyAsync(this TableClient tableClient, string partitionKey, ILogger? logger)
    {
        logger ??= NullLogger;
        logger.LogInformation(LogEvents.DeletePartitionStarted, "Deleting all entities in partition {PartitionKey} from table {TableName}.", partitionKey, tableClient.Name);

        // Only the PartitionKey & RowKey fields are required for deletion
        AsyncPageable<TableEntity> entities = tableClient
            .QueryAsync<TableEntity>(x => x.PartitionKey == partitionKey, select: ["PartitionKey", "RowKey"], maxPerPage: 1000);

        int totalDeleted = 0;
        await foreach (Page<TableEntity> page in entities.AsPages())
        {
            // Since we don't know how many rows the table has and the results are ordered by PartitonKey+RowKey
            // we'll delete each page immediately and not cache the whole table in memory
            logger.LogDebug(LogEvents.DeletePartitionPage, "Deleting page with {EntityCount} entities in partition {PartitionKey} from table {TableName}.", page.Values.Count, partitionKey, tableClient.Name);
            await BatchManipulateEntities(tableClient, page.Values, TableTransactionActionType.Delete, logger).ConfigureAwait(false);
            totalDeleted += page.Values.Count;
        }
        logger.LogInformation(LogEvents.DeletePartitionCompleted, "Completed deleting entities in partition {PartitionKey} from table {TableName}. Deleted entities: {DeletedEntityCount}.", partitionKey, tableClient.Name, totalDeleted);
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
        string? filter = partitionKey is null ? null : TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey}");
        IAsyncEnumerable<Page<TableEntity>> pages = tableClient
            .QueryAsync<TableEntity>(filter: filter, select: ["PartitionKey"], maxPerPage: 1000)
            .AsPages();

        int count = 0;
        await foreach (Page<TableEntity> page in pages)
        {
            count += page.Values.Count;
        }
        return count;
    }
}
