using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Medienstudio.Azure.Data.Tables.Extensions;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;

namespace Medienstudio.Azure.Data.Tables.JSON.Tests;

[TestClass]
public class ExtensionTests
{
    private const string ConnectionString = "UseDevelopmentStorage=true";

    private TableServiceClient _tableServiceClient = null!;
    private TableClient _tableClient = null!;

    private const string specialChars = "äöüßÄÖÜ#-.;:_!§$%&/()=?`´*'+~<>|@€{[]}\\^°²³";

    [TestInitialize]
    public void Initialize()
    {
        _tableServiceClient = new TableServiceClient(ConnectionString);
        _tableClient = _tableServiceClient.GetTableClient(RandomTableName());
        _tableClient.CreateIfNotExists();
    }

    [TestMethod]
    public async Task TestExportFile()
    {
        CreateTestData();
        using StreamWriter writer = File.CreateText("test.ndjson");
        await _tableClient.ExportJSONAsync(writer);
        Assert.IsTrue(File.Exists("test.ndjson"));

        string[] lines = File.ReadAllLines("test.ndjson");
        Assert.AreEqual(10, lines.Length);

        JsonObject binary = ParseLine(lines, "01-binary");
        Assert.AreEqual("partition", binary["PartitionKey"]!.GetValue<string>());
        Assert.AreEqual("YmluYXJ5", binary["binary"]!.GetValue<string>());
        Assert.AreEqual("Binary", binary["binary@type"]!.GetValue<string>());

        JsonObject boolRow = ParseLine(lines, "02-bool");
        Assert.IsTrue(boolRow["bool"]!.GetValue<bool>());
        Assert.AreEqual("Boolean", boolRow["bool@type"]!.GetValue<string>());

        JsonObject dateTime = ParseLine(lines, "03-datetime");
        Assert.AreEqual("2020-01-01T01:01:01Z", dateTime["datetime"]!.GetValue<string>());
        Assert.AreEqual("DateTime", dateTime["datetime@type"]!.GetValue<string>());

        JsonObject dateTimeOffset = ParseLine(lines, "04-datetimeoffset");
        Assert.AreEqual("2019-12-31T23:01:01Z", dateTimeOffset["datetimeoffset"]!.GetValue<string>());
        Assert.AreEqual("DateTime", dateTimeOffset["datetimeoffset@type"]!.GetValue<string>());

        JsonObject doubleRow = ParseLine(lines, "05-double");
        Assert.AreEqual("1.1", doubleRow["double"]!.ToJsonString());
        Assert.AreEqual("Double", doubleRow["double@type"]!.GetValue<string>());

        JsonObject guid = ParseLine(lines, "06-guid");
        Assert.IsTrue(Guid.TryParse(guid["guid"]!.GetValue<string>(), out _));
        Assert.AreEqual("Guid", guid["guid@type"]!.GetValue<string>());

        JsonObject intRow = ParseLine(lines, "07-int");
        Assert.AreEqual(1, intRow["int"]!.GetValue<int>());
        Assert.AreEqual("Int32", intRow["int@type"]!.GetValue<string>());

        JsonObject longRow = ParseLine(lines, "08-long");
        Assert.AreEqual(1L, longRow["long"]!.GetValue<long>());
        Assert.AreEqual("Int64", longRow["long@type"]!.GetValue<string>());

        JsonObject specialCharsRow = ParseLine(lines, "09-specialChars");
        Assert.AreEqual(specialChars, specialCharsRow["specialChars"]!.GetValue<string>());
        Assert.AreEqual("String", specialCharsRow["specialChars@type"]!.GetValue<string>());

        JsonObject quotesRow = ParseLine(lines, "10-quotes");
        Assert.AreEqual("string with \"quotes\"", quotesRow["quotes"]!.GetValue<string>());
        Assert.AreEqual("String", quotesRow["quotes@type"]!.GetValue<string>());
    }

    [TestMethod]
    public async Task TestExportAzureBlob()
    {
        CreateTestData();
        BlobContainerClient containerClient = new(ConnectionString, "testcontainer");
        containerClient.CreateIfNotExists();
        BlobClient blobClient = containerClient.GetBlobClient("test.ndjson");

        Stream stream = await blobClient.OpenWriteAsync(true, new BlobOpenWriteOptions() { HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-ndjson" } });
        using StreamWriter writer = new(stream);

        await _tableClient.ExportJSONAsync(writer);
        Assert.IsTrue(await blobClient.ExistsAsync());
    }

    [TestMethod]
    public async Task TestExportInMemory()
    {
        CreateTestData();
        string defaultExport = await ExportToStringAsync(writer => _tableClient.ExportJSONAsync(writer));
        string inMemoryExport = await ExportToStringAsync(writer => _tableClient.ExportJSONInMemoryAsync(writer));

        Assert.AreEqual(defaultExport, inMemoryExport);

        string[] lines = inMemoryExport.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.AreEqual(10, lines.Length);
    }

    [TestMethod]
    public async Task TestExportEmptyTable()
    {
        string defaultExport = await ExportToStringAsync(writer => _tableClient.ExportJSONAsync(writer));
        string inMemoryExport = await ExportToStringAsync(writer => _tableClient.ExportJSONInMemoryAsync(writer));

        Assert.AreEqual(string.Empty, defaultExport);
        Assert.AreEqual(defaultExport, inMemoryExport);
    }

    [TestMethod]
    public async Task TestExportExcludesODataEtag()
    {
        TableEntity entity = new("partition", "etag")
        {
            { "odata.etag", "ignored" },
            { "value", "included" }
        };
        _tableClient.AddEntity(entity);

        string export = await ExportToStringAsync(writer => _tableClient.ExportJSONAsync(writer));
        Assert.IsFalse(export.Contains("odata.etag", StringComparison.Ordinal));

        JsonObject row = JsonNode.Parse(export.Trim())!.AsObject();
        Assert.AreEqual("included", row["value"]!.GetValue<string>());
    }

    [TestMethod]
    public async Task TestImportFile()
    {
        TableClient sourceTableClient = _tableServiceClient.GetTableClient(RandomTableName());
        sourceTableClient.CreateIfNotExists();
        string sourceJson;
        try
        {
            CreateTestData(sourceTableClient);
            sourceJson = await ExportToStringAsync(writer => sourceTableClient.ExportJSONAsync(writer));
        }
        finally
        {
            sourceTableClient.Delete();
        }

        using StringReader reader = new(sourceJson);
        await _tableClient.ImportJSONAsync(reader);

        string outputJson = await ExportToStringAsync(writer => _tableClient.ExportJSONAsync(writer));

        string[] sourceLines = sourceJson.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        string[] outputLines = outputJson.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.AreEqual(sourceLines.Length, outputLines.Length);

        for (int i = 0; i < sourceLines.Length; i++)
        {
            JsonObject sourceRow = JsonNode.Parse(sourceLines[i])!.AsObject();
            JsonObject outputRow = JsonNode.Parse(outputLines[i])!.AsObject();

            foreach (KeyValuePair<string, JsonNode?> property in sourceRow)
            {
                if (property.Key == "Timestamp")
                {
                    continue;
                }

                Assert.AreEqual(property.Value?.ToJsonString(), outputRow[property.Key]?.ToJsonString(), $"Mismatch for property '{property.Key}'");
            }
        }
    }

    [TestMethod]
    public async Task TestImportBatch()
    {
        StringBuilder builder = new();
        for (int i = 0; i < 250; i++)
        {
            builder.AppendLine($$"""{"PartitionKey":"partition","RowKey":"row-{{i:0000}}","value":{{i}},"value@type":"Int32"}""");
        }

        using StringReader reader = new(builder.ToString());
        await _tableClient.ImportJSONAsync(reader);

        List<TableEntity> rows = await _tableClient.GetAllEntitiesAsync<TableEntity>();
        Assert.AreEqual(250, rows.Count);
    }

    [TestMethod]
    public async Task TestImportPreservesEmptyValues()
    {
        const string jsonLine = """{"PartitionKey":"partition","RowKey":"empty","emptyString":"","emptyString@type":"String","emptyBinary":"","emptyBinary@type":"Binary","emptyInt32":null,"emptyInt32@type":"Int32"}""";
        using StringReader reader = new(jsonLine);

        await _tableClient.ImportJSONAsync(reader);

        TableEntity entity = (await _tableClient.GetEntityAsync<TableEntity>("partition", "empty")).Value;
        Assert.AreEqual(string.Empty, entity.GetString("emptyString"));
        CollectionAssert.AreEqual(Array.Empty<byte>(), entity.GetBinary("emptyBinary"));
        Assert.IsFalse(entity.ContainsKey("emptyInt32"));
    }

    private static JsonObject ParseLine(string[] lines, string rowKey)
    {
        foreach (string line in lines)
        {
            JsonObject row = JsonNode.Parse(line)!.AsObject();
            if (row["RowKey"]?.GetValue<string>() == rowKey)
            {
                return row;
            }
        }

        throw new InvalidOperationException($"No exported row found for RowKey '{rowKey}'.");
    }

    private static string RandomTableName()
    {
        return "t" + Guid.NewGuid().ToString("N");
    }

    private static async Task<string> ExportToStringAsync(Func<TextWriter, Task> export)
    {
        using StringWriter writer = new(CultureInfo.InvariantCulture);
        await export(writer);
        return writer.ToString();
    }

    private void CreateTestData(TableClient? targetTableClient = null)
    {
        TableClient tableClient = targetTableClient ?? _tableClient;
        if (tableClient is null)
            return;

        // supported property types
        // https://learn.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model#property-types

        // binary
        TableEntity binaryEntity = new("partition", "01-binary");
        byte[] binary = Encoding.UTF8.GetBytes("binary");
        binaryEntity.Add("binary", binary);
        tableClient.AddEntity(binaryEntity);

        // bool
        TableEntity boolEntity = new("partition", "02-bool")
        {
            { "bool", true }
        };
        tableClient.AddEntity(boolEntity);

        // datetime
        TableEntity dateTimeEntity = new("partition", "03-datetime");
        DateTime dateTime = new(2020, 1, 1, 1, 1, 1, DateTimeKind.Utc);
        dateTimeEntity.Add("datetime", dateTime);
        tableClient.AddEntity(dateTimeEntity);

        // datetimeoffset
        TableEntity dateTimeOffsetEntity = new("partition", "04-datetimeoffset");
        DateTimeOffset dateTimeOffset = new(2020, 1, 1, 1, 1, 1, TimeSpan.FromHours(2));
        dateTimeOffsetEntity.Add("datetimeoffset", dateTimeOffset);
        tableClient.AddEntity(dateTimeOffsetEntity);

        // double
        TableEntity doubleEntity = new("partition", "05-double")
        {
            { "double", 1.1 }
        };
        tableClient.AddEntity(doubleEntity);

        // guid
        TableEntity guidEntity = new("partition", "06-guid");
        Guid guid = Guid.NewGuid();
        guidEntity.Add("guid", guid);
        tableClient.AddEntity(guidEntity);

        // int32
        TableEntity intEntity = new("partition", "07-int")
        {
            { "int", 1 }
        };
        tableClient.AddEntity(intEntity);

        // int64
        TableEntity longEntity = new("partition", "08-long")
        {
            { "long", 1L }
        };
        tableClient.AddEntity(longEntity);

        // special chars
        TableEntity specialCharsEntity = new("partition", "09-specialChars")
        {
            { "specialChars", specialChars }
        };
        tableClient.AddEntity(specialCharsEntity);

        // quotes
        TableEntity quotesEntity = new("partition", "10-quotes")
        {
            { "quotes",  "string with \"quotes\""}
        };
        tableClient.AddEntity(quotesEntity);
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (_tableClient is null)
            return;

        _tableClient.Delete();
    }
}
