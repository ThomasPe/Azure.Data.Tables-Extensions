using Azure;
using Azure.Data.Tables;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Medienstudio.Azure.Data.Tables.Extensions
{
    /// <summary>
    /// Class providing Azure Data Table Extension methods
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Groups entities by PartitionKey into batches of max 100 for valid transactions
        /// </summary>
        private static async Task BatchManipulateEntities<T>(TableClient tableClient, IEnumerable<T> entities, TableTransactionActionType tableTransactionActionType) where T : class, ITableEntity, new()
        {
            var groups = entities.GroupBy(x => x.PartitionKey);
            foreach (var group in groups)
            {
                var items = group.AsEnumerable();
                while (items.Any())
                {
                    var batch = items.Take(100);
                    items = items.Skip(100);

                    var actions = new List<TableTransactionAction>();
                    actions.AddRange(batch.Select(e => new TableTransactionAction(tableTransactionActionType, e)));
                    await tableClient.SubmitTransactionAsync(actions);
                }
            }
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
            return await tableClient.QueryAsync<T>(x => x.RowKey == rowKey).ToListAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Returns all rows in the table
        /// </summary>
        /// <typeparam name="T">Implementation of ITableEntity</typeparam>
        /// <param name="tableClient">The authenticated TableClient</param>
        /// <returns></returns>
        public static async Task<IList<T>> GetAllEntitiesAsync<T>(this TableClient tableClient) where T : class, ITableEntity, new()
        {
            return await tableClient.QueryAsync<T>(maxPerPage: 1000).ToListAsync().ConfigureAwait(false);
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
            await BatchManipulateEntities(tableClient, entities, tableTransactionActionType);
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
                .QueryAsync<TableEntity>(select: new List<string>() { "PartitionKey", "RowKey" }, maxPerPage: 1000);

            await entities.AsPages().ForEachAwaitAsync(async page => {
                // Since we don't know how many rows the table has and the results are ordered by PartitonKey+RowKey
                // we'll delete each page immediately and not cache the whole table in memory
                await BatchManipulateEntities(tableClient, page.Values, TableTransactionActionType.Delete);
            });
        } 
    }
}
