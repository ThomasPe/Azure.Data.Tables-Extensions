using Azure.Data.Tables;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Medienstudio.Azure.Data.Tables.CSV
{
    public static class Extensions
    {
        const string TYPE_SUFFIX = "@type";

        // extension method for TableClient to export all rows as CSV
        public static async Task ExportAsCSV(this TableClient tableClient, TextWriter writer)
        {
            List<TableEntity> rows = new(0);
            List<string> systemProperties = new(3) { "PartitionKey", "RowKey", "Timestamp" };
            List<string> keys = new(3);
            keys.AddRange(systemProperties);
            List<string> ignore = new(1) { "odata.etag" };

            using CsvWriter csv = new(writer, CultureInfo.InvariantCulture);

            // preserve milliseconds, truncate trailing zeros
            csv.Context.TypeConverterOptionsCache.GetOptions<DateTime>().Formats = new string[] { "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ" };
            csv.Context.TypeConverterOptionsCache.GetOptions<DateTimeOffset>().Formats = new string[] { "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ" };


            // serialize byte arrays as base64 strings
            csv.Context.TypeConverterCache.AddConverter<byte[]>(new BinaryConverter());

            // serilaize booleans lowercase
            csv.Context.TypeConverterCache.AddConverter<bool>(new BoolConverter());


            var query = tableClient.QueryAsync<TableEntity>().AsPages();

            // loop through all result pages
            await foreach (var page in query)
            {
                // loop through all rows in the page
                foreach (var entity in page.Values)
                {
                    // cache all rows
                    rows.Add(entity);

                    // prepare the list of keys for csv header
                    foreach (var property in entity)
                    {
                        // skip properties that should be ignored for the export like odata.etag
                        if (ignore.Contains(property.Key))
                        {
                            continue;
                        }
                        // add the property key to the list of keys if it is not already in the list
                        if (!keys.Contains(property.Key))
                        {
                            keys.Add(property.Key);
                        }
                        // add the type of the property to the list of keys if it is not a system property
                        if (!systemProperties.Contains(property.Key) && !keys.Contains(property.Key + "@type"))
                        {
                            keys.Add(property.Key + "@type");
                        }
                    }
                }
            }

            // create the csv header
            foreach (var key in keys)
            {
                csv.WriteField(key);
            }
            csv.NextRecord();

            // create the csv rows
            foreach (var row in rows)
            {
                foreach (var key in keys)
                {
                    if (row.TryGetValue(key, out var value))
                    {
                        // write the value of the property
                        csv.WriteField(value);
                    }
                    else if (key.EndsWith(TYPE_SUFFIX))
                    {
                        // write the type of the property
                        if (row.ContainsKey(key.Substring(0, key.Length - TYPE_SUFFIX.Length))){
                            csv.WriteField(row[key.Substring(0, key.Length - TYPE_SUFFIX.Length)].GetPropertyTypeName());
                        }
                        else
                        {
                            csv.WriteField("");
                        }
                        
                    }
                    else
                    {
                        // write an empty field
                        csv.WriteField("");
                    }
                }
                csv.NextRecord();
            }

            csv.Flush();
        }
    }
}
