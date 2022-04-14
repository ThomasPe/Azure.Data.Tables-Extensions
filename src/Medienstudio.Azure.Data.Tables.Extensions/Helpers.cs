using System;
using System.Text;

namespace Medienstudio.Azure.Data.Tables.Extensions
{
    /// <summary>
    /// Set of Azure Table Storage related helper methods
    /// </summary>
    public static class Helpers
    {
        /// <summary>
        /// Returns an ever-decreasing number as string to be used as PartitionKey or RowKey.
        /// Ensures that newly created entities are always "on top" and returned first in queries
        /// </summary>
        /// <returns></returns>
        public static string TicksKey()
        {
            return (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19");
        }

        /// <summary>
        /// Converts TicksKey back to DateTimeOffset
        /// </summary>
        /// <param name="ticksKey"></param>
        /// <returns></returns>
        public static DateTimeOffset TicksKeyToDateTimeOffset(string ticksKey)
        {
           return new DateTimeOffset(DateTime.MaxValue.Ticks - long.Parse(ticksKey), TimeSpan.Zero);
        }

        /// <summary>
        /// Converts a string into a PartitionKey/RowKey-safe base64 string
        /// <see href="https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#characters-disallowed-in-key-fields">Characters Disallowed in Key Fields</see>
        /// </summary>
        /// <param name="key">Key to be converted</param>
        /// <returns>Key-safe string</returns>
        public static string ToSafeKey(string key) => Convert.ToBase64String(Encoding.UTF8.GetBytes(key)).Replace('/', '.');
        
        /// <summary>
        /// Decodes the Key field
        /// </summary>
        /// <param name="key">Key-safe base64 encoded string</param>
        /// <returns>Original Key</returns>
        public static string FromSafeKey(string key) => Encoding.UTF8.GetString(Convert.FromBase64String(key.Replace('.', '/')));
    }
}
