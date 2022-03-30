using System;

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
    }
}
