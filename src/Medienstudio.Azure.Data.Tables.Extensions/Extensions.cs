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
        /// Returns all rows for a given RowKey
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableClient">The authenticated TableClient</param>
        /// <param name="rowKey">The RowKey</param>
        /// <remarks>Will result in a Table Scan which can result in bad query performance</remarks>
        /// <returns></returns>
        public static async Task<IList<T>> GetByRowKeyAsync<T>(this TableClient tableClient, string rowKey) where T : class, ITableEntity, new()
        {
            return await tableClient.QueryAsync<T>(x => x.RowKey == rowKey).ToListAsync();
        }
    }
}
