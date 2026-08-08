using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using Microsoft.Extensions.Logging;
using Medienstudio.Azure.Data.Tables.Extensions;
using System.Globalization;
using System.Text;

namespace Medienstudio.Azure.Data.Tables.CSV.Tests;

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
        using StreamWriter writer = File.CreateText("test.csv");
        await _tableClient.ExportCSVAsync(writer);
        Assert.IsTrue(File.Exists("test.csv"));

        string[] lines = File.ReadAllLines("test.csv");
        Assert.AreEqual(11, lines.Length);

        // header
        Assert.AreEqual("PartitionKey,RowKey,Timestamp,binary,binary@type,bool,bool@type,datetime,datetime@type,datetimeoffset,datetimeoffset@type,double,double@type,guid,guid@type,int,int@type,long,long@type,specialChars,specialChars@type,quotes,quotes@type", lines[0]);

        // data
        string[] dataBinary = lines[1].Split(',');
        Assert.AreEqual("partition", dataBinary[0]);
        Assert.AreEqual("01-binary", dataBinary[1]);
        Assert.AreEqual("YmluYXJ5", dataBinary[3]);
        Assert.AreEqual("Binary", dataBinary[4]);

        string[] dataBool = lines[2].Split(',');
        Assert.AreEqual("partition", dataBool[0]);
        Assert.AreEqual("02-bool", dataBool[1]);
        Assert.AreEqual("true", dataBool[5]);
        Assert.AreEqual("Boolean", dataBool[6]);

        string[] dataDateTime = lines[3].Split(',');
        Assert.AreEqual("partition", dataDateTime[0]);
        Assert.AreEqual("2020-01-01T01:01:01Z", dataDateTime[7]);
        Assert.AreEqual("DateTime", dataDateTime[8]);

        string[] dataDateTimeOffset = lines[4].Split(',');
        Assert.AreEqual("partition", dataDateTimeOffset[0]);
        Assert.AreEqual("2019-12-31T23:01:01Z", dataDateTimeOffset[9]);
        Assert.AreEqual("DateTime", dataDateTimeOffset[10]);

        string[] dataDouble = lines[5].Split(',');
        Assert.AreEqual("partition", dataDouble[0]);
        Assert.AreEqual("05-double", dataDouble[1]);
        Assert.AreEqual("1.1", dataDouble[11]);
        Assert.AreEqual("Double", dataDouble[12]);

        string[] dataGuid = lines[6].Split(',');
        Assert.AreEqual("partition", dataGuid[0]);
        Assert.AreEqual("06-guid", dataGuid[1]);
        Assert.IsTrue(Guid.TryParse(dataGuid[13], out _));
        Assert.AreEqual("Guid", dataGuid[14]);

        string[] dataInt = lines[7].Split(',');
        Assert.AreEqual("partition", dataInt[0]);
        Assert.AreEqual("07-int", dataInt[1]);
        Assert.AreEqual("1", dataInt[15]);
        Assert.AreEqual("Int32", dataInt[16]);

        string[] dataLong = lines[8].Split(',');
        Assert.AreEqual("partition", dataLong[0]);
        Assert.AreEqual("08-long", dataLong[1]);
        Assert.AreEqual("1", dataLong[17]);
        Assert.AreEqual("Int64", dataLong[18]);

        string[] dataSpecialChars = lines[9].Split(',');
        Assert.AreEqual("partition", dataSpecialChars[0]);
        Assert.AreEqual("09-specialChars", dataSpecialChars[1]);
        Assert.AreEqual(specialChars, dataSpecialChars[19]);
        Assert.AreEqual("String", dataSpecialChars[20]);

        string[] dataQuotes = lines[10].Split(',');
        Assert.AreEqual("partition", dataQuotes[0]);
        Assert.AreEqual("10-quotes", dataQuotes[1]);
        // string is wrapped in quotes and included quotes are escaped with double quotes ""
        Assert.AreEqual("\"string with \"\"quotes\"\"\"", dataQuotes[21]);
        Assert.AreEqual("String", dataQuotes[22]);
    }

    [TestMethod]
    public async Task TestExportAzureBlob()
    {
        CreateTestData();
        BlobContainerClient containerClient = new(ConnectionString, "testcontainer");
        containerClient.CreateIfNotExists();
        BlobClient blobClient = containerClient.GetBlobClient("test.csv");

        Stream stream = await blobClient.OpenWriteAsync(true, new BlobOpenWriteOptions() { HttpHeaders = new BlobHttpHeaders { ContentType = "text/csv" } });
        using StreamWriter writer = new(stream);

        await _tableClient.ExportCSVAsync(writer);
        Assert.IsTrue(await blobClient.ExistsAsync());
    }

    [TestMethod]
    public async Task TestExportWithSchema()
    {
        CreateTestData();
        CsvExportSchema schema = await _tableClient.GetCSVExportSchemaAsync();
        CollectionAssert.AreEqual(new[] { "binary", "bool", "datetime", "datetimeoffset", "double", "guid", "int", "long", "specialChars", "quotes" }, schema.Properties.ToArray());

        string defaultExport = await ExportToStringAsync(writer => _tableClient.ExportCSVAsync(writer));
        string schemaExport = await ExportToStringAsync(writer => _tableClient.ExportCSVAsync(writer, schema));

        string header = schemaExport.Split("\r\n")[0];
        Assert.AreEqual("PartitionKey,RowKey,Timestamp,binary,binary@type,bool,bool@type,datetime,datetime@type,datetimeoffset,datetimeoffset@type,double,double@type,guid,guid@type,int,int@type,long,long@type,specialChars,specialChars@type,quotes,quotes@type", header);
        Assert.AreEqual(defaultExport, schemaExport);
    }

    [TestMethod]
    public async Task TestExportInMemory()
    {
        CreateTestData();
        string defaultExport = await ExportToStringAsync(writer => _tableClient.ExportCSVAsync(writer));
        string inMemoryExport = await ExportToStringAsync(writer => _tableClient.ExportCSVInMemoryAsync(writer));

        string[] lines = inMemoryExport.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        Assert.AreEqual(11, lines.Length);
        Assert.AreEqual("PartitionKey,RowKey,Timestamp,binary,binary@type,bool,bool@type,datetime,datetime@type,datetimeoffset,datetimeoffset@type,double,double@type,guid,guid@type,int,int@type,long,long@type,specialChars,specialChars@type,quotes,quotes@type", lines[0]);
        Assert.AreEqual(defaultExport, inMemoryExport);
    }

    [TestMethod]
    public async Task TestExportEmptyTable()
    {
        CsvExportSchema schema = await _tableClient.GetCSVExportSchemaAsync();
        Assert.AreEqual(0, schema.Properties.Count);

        string defaultExport = await ExportToStringAsync(writer => _tableClient.ExportCSVAsync(writer));
        string schemaExport = await ExportToStringAsync(writer => _tableClient.ExportCSVAsync(writer, schema));
        string inMemoryExport = await ExportToStringAsync(writer => _tableClient.ExportCSVInMemoryAsync(writer));

        Assert.AreEqual("PartitionKey,RowKey,Timestamp\r\n", defaultExport);
        Assert.AreEqual(defaultExport, schemaExport);
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

        CsvExportSchema schema = await _tableClient.GetCSVExportSchemaAsync();
        CollectionAssert.AreEqual(new[] { "value" }, schema.Properties.ToArray());

        string export = await ExportToStringAsync(writer => _tableClient.ExportCSVAsync(writer));
        Assert.IsFalse(export.Contains("odata.etag", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task TestExportDetectsSchemaDrift()
    {
        _tableClient.AddEntity(new TableEntity("partition", "initial") { { "known", "value" } });
        using MutatingStringWriter writer = new(() => _tableClient.AddEntity(new TableEntity("partition", "new") { { "unknown", "value" } }));

        await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => _tableClient.ExportCSVAsync(writer));
    }

    [TestMethod]
    public async Task TestExportWithIncompleteSchema()
    {
        TableEntity entity = new("partition", "incomplete")
        {
            { "value", 1 }
        };
        _tableClient.AddEntity(entity);

        await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => _tableClient.ExportCSVAsync(TextWriter.Null, new CsvExportSchema([])));
    }

    [TestMethod]
    public async Task TestStoredCsvExportSchema()
    {
        TableClient metadataTableClient = _tableServiceClient.GetTableClient(RandomTableName());
        metadataTableClient.CreateIfNotExists();
        try
        {
            Assert.IsNull(await _tableClient.GetStoredCSVExportSchemaAsync(metadataTableClient));

            CsvExportSchema schema = new(["first", "second"]);
            await _tableClient.StoreCSVExportSchemaAsync(metadataTableClient, schema);

            CsvExportSchema? storedSchema = await _tableClient.GetStoredCSVExportSchemaAsync(metadataTableClient);
            Assert.IsNotNull(storedSchema);
            CollectionAssert.AreEqual(schema.Properties.ToArray(), storedSchema.Properties.ToArray());

            CsvExportSchema replacementSchema = new(["replacement"]);
            await _tableClient.StoreCSVExportSchemaAsync(metadataTableClient, replacementSchema);

            storedSchema = await _tableClient.GetStoredCSVExportSchemaAsync(metadataTableClient);
            Assert.IsNotNull(storedSchema);
            CollectionAssert.AreEqual(replacementSchema.Properties.ToArray(), storedSchema.Properties.ToArray());
        }
        finally
        {
            metadataTableClient.Delete();
        }
    }

    [TestMethod]
    public async Task TestStoredCsvExportSchemaRequiresSeparateTable()
    {
        CsvExportSchema schema = new([]);

        await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => _tableClient.StoreCSVExportSchemaAsync(_tableClient, schema));
        await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => _tableClient.GetStoredCSVExportSchemaAsync(_tableClient));
    }

    [TestMethod]
    public async Task TestGetStoredCsvExportSchemaThrowsWhenSchemaJsonMissing()
    {
        TableClient metadataTableClient = _tableServiceClient.GetTableClient(RandomTableName());
        metadataTableClient.CreateIfNotExists();
        try
        {
            await _tableClient.StoreCSVExportSchemaAsync(metadataTableClient, new CsvExportSchema(["value"]));
            TableEntity metadata = await GetSingleMetadataEntityAsync(metadataTableClient);

            // replace the entity without a SchemaJson property
            await metadataTableClient.UpsertEntityAsync(new TableEntity(metadata.PartitionKey, metadata.RowKey), TableUpdateMode.Replace);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => _tableClient.GetStoredCSVExportSchemaAsync(metadataTableClient));
        }
        finally
        {
            metadataTableClient.Delete();
        }
    }

    [TestMethod]
    public async Task TestGetStoredCsvExportSchemaThrowsWhenSchemaJsonIsMalformed()
    {
        TableClient metadataTableClient = _tableServiceClient.GetTableClient(RandomTableName());
        metadataTableClient.CreateIfNotExists();
        try
        {
            await _tableClient.StoreCSVExportSchemaAsync(metadataTableClient, new CsvExportSchema(["value"]));
            TableEntity metadata = await GetSingleMetadataEntityAsync(metadataTableClient);

            TableEntity corrupted = new(metadata.PartitionKey, metadata.RowKey) { ["SchemaJson"] = "not-json" };
            await metadataTableClient.UpsertEntityAsync(corrupted, TableUpdateMode.Replace);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => _tableClient.GetStoredCSVExportSchemaAsync(metadataTableClient));
        }
        finally
        {
            metadataTableClient.Delete();
        }
    }

    [TestMethod]
    public async Task TestGetStoredCsvExportSchemaThrowsWhenStoredPropertiesAreInvalid()
    {
        TableClient metadataTableClient = _tableServiceClient.GetTableClient(RandomTableName());
        metadataTableClient.CreateIfNotExists();
        try
        {
            await _tableClient.StoreCSVExportSchemaAsync(metadataTableClient, new CsvExportSchema(["value"]));
            TableEntity metadata = await GetSingleMetadataEntityAsync(metadataTableClient);

            // "PartitionKey" is a reserved property name that CsvExportSchema rejects
            TableEntity corrupted = new(metadata.PartitionKey, metadata.RowKey) { ["SchemaJson"] = "[\"PartitionKey\"]" };
            await metadataTableClient.UpsertEntityAsync(corrupted, TableUpdateMode.Replace);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => _tableClient.GetStoredCSVExportSchemaAsync(metadataTableClient));
        }
        finally
        {
            metadataTableClient.Delete();
        }
    }

    private static async Task<TableEntity> GetSingleMetadataEntityAsync(TableClient metadataTableClient)
    {
        await foreach (TableEntity entity in metadataTableClient.QueryAsync<TableEntity>())
        {
            return entity;
        }

        throw new InvalidOperationException("Expected a stored metadata entity.");
    }

    [TestMethod]
    public void TestCsvExportSchemaValidation()
    {
        Assert.ThrowsException<ArgumentNullException>(() => new CsvExportSchema(null!));
        Assert.ThrowsException<ArgumentException>(() => new CsvExportSchema(new string[] { null! }));
        Assert.ThrowsException<ArgumentException>(() => new CsvExportSchema([" "]));
        Assert.ThrowsException<ArgumentException>(() => new CsvExportSchema(["value", "value"]));
        Assert.ThrowsException<ArgumentException>(() => new CsvExportSchema(["PartitionKey"]));
        Assert.ThrowsException<ArgumentException>(() => new CsvExportSchema(["RowKey"]));
        Assert.ThrowsException<ArgumentException>(() => new CsvExportSchema(["Timestamp"]));
        Assert.ThrowsException<ArgumentException>(() => new CsvExportSchema(["odata.etag"]));
    }

    [TestMethod]
    public async Task TestImportFile()
    {
        TableClient sourceTableClient = _tableServiceClient.GetTableClient(RandomTableName());
        sourceTableClient.CreateIfNotExists();
        string sourceCsv;
        try
        {
            CreateTestData(sourceTableClient);
            sourceCsv = await ExportToStringAsync(writer => sourceTableClient.ExportCSVAsync(writer));
        }
        finally
        {
            sourceTableClient.Delete();
        }

        using StringReader reader = new(sourceCsv);
        await _tableClient.ImportCSVAsync(reader);

        string outputCsv = await ExportToStringAsync(writer => _tableClient.ExportCSVAsync(writer));

        using StringReader reader1 = new(sourceCsv);
        using CsvReader csv1 = new(reader1, CultureInfo.InvariantCulture);
        csv1.Read();
        csv1.ReadHeader();

        using StringReader reader2 = new(outputCsv);
        using CsvReader csv2 = new(reader2, CultureInfo.InvariantCulture);
        csv2.Read();
        csv2.ReadHeader();

        while (csv1.Read())
        {
            csv2.Read();

            int i = 0;
            while (csv1.TryGetField(i, out string? field1))
            {
                string? label1 = csv1.HeaderRecord?[i];
                if (label1 == "Timestamp")
                {
                    i++;
                    continue;
                }
                string? field2 = csv2.GetField(i);
                Assert.AreEqual(field1, field2);
                i++;
            }
        }
    }

    [TestMethod]
    public async Task TestImportBatch()
    {
        using StreamReader reader = new("test-batch.csv");
        await _tableClient.ImportCSVAsync(reader);
        List<TableEntity> rows = await _tableClient.GetAllEntitiesAsync<TableEntity>();
        Assert.AreEqual(3003, rows.Count);
    }

    [TestMethod]
    public async Task TestImportPreservesEmptyValues()
    {
        const string csvContent = "PartitionKey,RowKey,emptyString,emptyString@type,emptyBinary,emptyBinary@type,emptyInt32,emptyInt32@type\r\npartition,empty,,String,,Binary,,Int32\r\n";
        using StringReader reader = new(csvContent);

        await _tableClient.ImportCSVAsync(reader);

        TableEntity entity = (await _tableClient.GetEntityAsync<TableEntity>("partition", "empty")).Value;
        Assert.AreEqual(string.Empty, entity.GetString("emptyString"));
        CollectionAssert.AreEqual(Array.Empty<byte>(), entity.GetBinary("emptyBinary"));
        Assert.IsFalse(entity.ContainsKey("emptyInt32"));
    }

    [TestMethod]
    public async Task TestExportWithLoggingUsesDedicatedMethod()
    {
        using StringWriter writer = new(CultureInfo.InvariantCulture);
        TestLogger logger = new();

        await _tableClient.ExportCSVWithLoggingAsync(writer, logger);

        string informationLogs = string.Join(Environment.NewLine, logger.Entries.Where(entry => entry.Level == LogLevel.Information).Select(entry => entry.Message));
        Assert.IsTrue(informationLogs.Contains("Starting CSV export", StringComparison.Ordinal));
        Assert.IsTrue(informationLogs.Contains("Completed CSV export", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task TestImportTypeCoercionFailureLogsActionableContext()
    {
        const string csvContent = "PartitionKey,RowKey,value,value@type\r\npartition,row-1,not-an-int,Int32\r\n";
        using StringReader reader = new(csvContent);
        TestLogger logger = new();

        InvalidDataException exception = await Assert.ThrowsExceptionAsync<InvalidDataException>(() => _tableClient.ImportCSVAsync(reader, logger));
        Assert.IsTrue(exception.Message.Contains("row 2", StringComparison.Ordinal));
        Assert.IsTrue(exception.Message.Contains("column 'value'", StringComparison.Ordinal));

        string warningLog = string.Join(Environment.NewLine, logger.Entries.Where(x => x.Level == LogLevel.Warning).Select(x => x.Message));
        Assert.IsTrue(warningLog.Contains("type coercion error", StringComparison.Ordinal));
        Assert.IsTrue(warningLog.Contains("declared type Int32", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task TestImportDuplicateColumnLogsColumnError()
    {
        const string csvContent = "PartitionKey,RowKey,value,value@type,value,value@type\r\npartition,row-1,first,String,second,String\r\n";
        using StringReader reader = new(csvContent);
        TestLogger logger = new();

        InvalidDataException exception = await Assert.ThrowsExceptionAsync<InvalidDataException>(() => _tableClient.ImportCSVAsync(reader, logger));
        Assert.IsTrue(exception.Message.Contains("column 'value'", StringComparison.Ordinal));

        string warningLog = string.Join(Environment.NewLine, logger.Entries.Where(entry => entry.Level == LogLevel.Warning).Select(entry => entry.Message));
        Assert.IsTrue(warningLog.Contains("invalid column value", StringComparison.Ordinal));
        Assert.IsFalse(warningLog.Contains("type coercion error", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task TestImportMissingHeaderLogsWarning()
    {
        const string csvContent = "PartitionKey,RowKey\r\npartition,row-1,unexpected-extra-field\r\n";
        using StringReader reader = new(csvContent);
        TestLogger logger = new();

        InvalidDataException exception = await Assert.ThrowsExceptionAsync<InvalidDataException>(() => _tableClient.ImportCSVAsync(reader, logger));
        Assert.IsTrue(exception.Message.Contains("header", StringComparison.OrdinalIgnoreCase));

        string warningLog = string.Join(Environment.NewLine, logger.Entries.Where(x => x.Level == LogLevel.Warning).Select(x => x.Message));
        Assert.IsTrue(warningLog.Contains("missing header", StringComparison.OrdinalIgnoreCase));
    }

    [TestMethod]
    public async Task TestImportDoesNotEmitTableOperationLifecycleLogs()
    {
        const string csvContent = "PartitionKey,RowKey\r\npartition,row-1\r\n";
        using StringReader reader = new(csvContent);
        TestLogger logger = new();

        await _tableClient.ImportCSVAsync(reader, logger);

        string informationLogs = string.Join(Environment.NewLine, logger.Entries.Where(entry => entry.Level == LogLevel.Information).Select(entry => entry.Message));
        Assert.IsFalse(informationLogs.Contains("Adding entities", StringComparison.Ordinal));
        Assert.IsFalse(informationLogs.Contains("Starting batched table transaction", StringComparison.Ordinal));
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

    private sealed class MutatingStringWriter(Action mutation) : StringWriter
    {
        private Action? _mutation = mutation;

        public override void Write(char value)
        {
            Mutate();
            base.Write(value);
        }

        public override void Write(string? value)
        {
            Mutate();
            base.Write(value);
        }

        public override void Write(char[] buffer, int index, int count)
        {
            Mutate();
            base.Write(buffer, index, count);
        }

        public override void Write(ReadOnlySpan<char> buffer)
        {
            Mutate();
            base.Write(buffer);
        }

        private void Mutate()
        {
            Action? mutation = Interlocked.Exchange(ref _mutation, null);
            mutation?.Invoke();
        }
    }

    private sealed class TestLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];
        private static readonly IDisposable Scope = new NoopDisposable();

        IDisposable ILogger.BeginScope<TState>(TState state) => Scope;

        bool ILogger.IsEnabled(LogLevel logLevel) => true;

        void ILogger.Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Entries.Add((logLevel, formatter(state, exception)));
        }
    }

    private sealed class NoopDisposable : IDisposable
    {
        public void Dispose()
        {
        }
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (_tableClient is null)
            return;

        _tableClient.Delete();
    }
}