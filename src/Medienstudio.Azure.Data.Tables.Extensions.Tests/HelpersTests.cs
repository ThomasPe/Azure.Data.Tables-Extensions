using Azure.Data.Tables;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Medienstudio.Azure.Data.Tables.Extensions.Tests;

[TestClass]
public class HelpersTests
{
    [TestMethod]
    public void TicksKeyTests()
    {
        var ticksKey = Helpers.TicksKey();
        var convertedBack = Helpers.TicksKeyToDateTimeOffset(ticksKey);

        var diff = DateTimeOffset.Now - convertedBack;
        Assert.IsTrue(diff.TotalSeconds < 1);
    }

    [TestMethod]
    public void SafeKeyTests()
    {
        List<string> keys = new()
        {
            "/path/to/file.md",
            @"/\#?",
            "\n",
            " ",
            // should create a key including forward slash
            "subjects?_d=1"
        };

        foreach (var key in keys)
        {
            var safeKey = Helpers.ToSafeKey(key);
            var convertedBack = Helpers.FromSafeKey(safeKey);

            Assert.AreEqual(key, convertedBack);
            Assert.IsFalse(string.IsNullOrWhiteSpace(safeKey));
            // https://stackoverflow.com/questions/13195143/range-of-valid-character-for-a-base-64-encoding
            Assert.IsFalse(safeKey.Any(c => !(char.IsLetterOrDigit(c) || c == '.' || c == '/' || c == '+' || c == '=')));
        }

    }

    [TestMethod]
    public async Task CreateTestData()
    {
        // create table client 
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=tablestoragetest;AccountKey=YAYsqjyQGwO2ngoBEhiifgSc0JNgQLToVOidIcELAUaVS1EUN4iIMowIZCCcr2VTQRunB3huRjjKK1sFHrDswg==;EndpointSuffix=core.windows.net";
        var tableClient = new TableClient(connectionString, "test");
        await tableClient.CreateIfNotExistsAsync();

        //List<TableEntity> entites = new();

        //for (int j = 0; j < 20; j++)
        //{
        //    for (int i = 0; i < 2000; i++)
        //    {
        //        var e = new TableEntity()
        //        {
        //            PartitionKey = j.ToString(),
        //            RowKey = Guid.NewGuid().ToString(),
        //        };
        //        e["Test" + j] = "test";
        //        entites.Add(e);
        //    }
        //}
        //await tableClient.AddEntitiesAsync(entites, TableTransactionActionType.UpsertReplace);

        await tableClient.ExportToCsvAsync();

        Assert.IsTrue(true);
    }
}
