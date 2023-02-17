using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using Medienstudio.Azure.Data.Tables.Extensions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Globalization;
using System.Text;

namespace Medienstudio.Azure.Data.Tables.CSV.Tests
{
    [TestClass]
    public class ExtensionTests
    {
        private const string DefaultEndpointsProtocol = "http";
        private const string AccountName = "devstoreaccount1";
        private const string AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        
        private const string TableEndpoint = "http://127.0.0.1:10002/devstoreaccount1";
        private const string TableConnectionString = $"DefaultEndpointsProtocol={DefaultEndpointsProtocol};AccountName={AccountName};AccountKey={AccountKey};TableEndpoint={TableEndpoint};";

        private const string BlobEndpoint = "http://127.0.0.1:10000/" + AccountName;
        private const string BlobConnectionString = $"DefaultEndpointsProtocol={DefaultEndpointsProtocol};AccountName={AccountName};AccountKey={AccountKey};BlobEndpoint={BlobEndpoint};";
        
        private TableServiceClient? _tableServiceClient;
        private TableClient? _tableClient;

        [TestInitialize]
        public void Initialize()
        {
            _tableServiceClient = new TableServiceClient(TableConnectionString);
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

            var lines = File.ReadAllLines("test.csv");
            Assert.AreEqual(10, lines.Length);

            // header
            Assert.AreEqual("PartitionKey,RowKey,Timestamp,binary,binary@type,bool,bool@type,datetime,datetime@type,datetimeoffset,datetimeoffset@type,double,double@type,guid,guid@type,int,int@type,long,long@type,string,string@type", lines[0]);

            // data
            var dataBinary = lines[1].Split(',');
            Assert.AreEqual("partition", dataBinary[0]);
            Assert.AreEqual("binary", dataBinary[1]);
            Assert.AreEqual("YmluYXJ5", dataBinary[3]);
            Assert.AreEqual("Binary", dataBinary[4]);

            var dataBool = lines[2].Split(',');
            Assert.AreEqual("partition", dataBool[0]);
            Assert.AreEqual("bool", dataBool[1]);
            Assert.AreEqual("true", dataBool[5]);
            Assert.AreEqual("Boolean", dataBool[6]);

            var dataDateTime = lines[3].Split(',');
            Assert.AreEqual("partition", dataDateTime[0]);
            Assert.AreEqual("2020-01-01T01:01:01Z", dataDateTime[7]);
            Assert.AreEqual("DateTime", dataDateTime[8]);

            var dataDateTimeOffset = lines[4].Split(',');
            Assert.AreEqual("partition", dataDateTimeOffset[0]);
            Assert.AreEqual("2020-01-01T01:01:01Z", dataDateTimeOffset[9]);
            Assert.AreEqual("DateTime", dataDateTimeOffset[10]);

            var dataDouble = lines[5].Split(',');
            Assert.AreEqual("partition", dataDouble[0]);
            Assert.AreEqual("double", dataDouble[1]);
            Assert.AreEqual("1.1", dataDouble[11]);
            Assert.AreEqual("Double", dataDouble[12]);

            var dataGuid = lines[6].Split(',');
            Assert.AreEqual("partition", dataGuid[0]);
            Assert.AreEqual("guid", dataGuid[1]);
            Assert.IsTrue(Guid.TryParse(dataGuid[13], out _));
            Assert.AreEqual("Guid", dataGuid[14]);

            var dataInt = lines[7].Split(',');
            Assert.AreEqual("partition", dataInt[0]);
            Assert.AreEqual("int", dataInt[1]);
            Assert.AreEqual("1", dataInt[15]);
            Assert.AreEqual("Int32", dataInt[16]);
            
            var dataLong = lines[8].Split(',');
            Assert.AreEqual("partition", dataLong[0]);
            Assert.AreEqual("long", dataLong[1]);
            Assert.AreEqual("1", dataLong[17]);
            Assert.AreEqual("Int64", dataLong[18]);

            // TODO
            //var dataString = lines[9].Split(',');
            //Assert.AreEqual("partition", dataString[0]);
            //Assert.AreEqual("string,with,commas", dataString[19]);
            //Assert.AreEqual("String", dataString[20]);
        }

        [TestMethod]
        public async Task TestExportAzureBlob()
        {
            CreateTestData();
            BlobContainerClient containerClient = new(BlobConnectionString, "testcontainer");
            containerClient.CreateIfNotExists();
            var blobClient = containerClient.GetBlobClient("test.csv");

            var stream = await blobClient.OpenWriteAsync(true, new BlobOpenWriteOptions() { HttpHeaders = new BlobHttpHeaders { ContentType = "text/csv" } });
            using StreamWriter writer = new(stream);
            
            await _tableClient.ExportCSVAsync(writer);
            Assert.IsTrue(await blobClient.ExistsAsync());
        }

        [TestMethod]
        public async Task TestImportFile()
        {
            using StreamReader reader = new("test.csv");
            await _tableClient.ImportCSVAsync(reader);

            using StreamWriter writer = File.CreateText("output.csv");
            await _tableClient.ExportCSVAsync(writer);

            using StreamReader reader1 = new("test.csv");
            using var csv1 = new CsvReader(reader1, CultureInfo.InvariantCulture);
            csv1.Read();
            csv1.ReadHeader();

            using StreamReader reader2 = new("output.csv");
            using var csv2 = new CsvReader(reader2, CultureInfo.InvariantCulture);
            csv2.Read();
            csv2.ReadHeader();

            while (csv1.Read())
            {
                csv2.Read();

                int i = 0;
                while(csv1.TryGetField(i, out string? field1))
                {
                    string label1 = csv1.HeaderRecord[i];
                    if(label1 == "Timestamp")
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
            var rows = await _tableClient.GetAllEntitiesAsync<TableEntity>();
            Assert.AreEqual(3003, rows.Count);
        }

        private static string RandomTableName()
        {
            return "t" + Guid.NewGuid().ToString("N");
        }

        private void CreateTestData()
        {
            if (_tableClient is null)
                return;

            // supported property types
            // https://learn.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model#property-types

            // string
            var stringEntity = new TableEntity("partition", "string");
            stringEntity.Add("string", "string,with,commas");
            _tableClient.AddEntity(stringEntity);

            // int32
            var intEntity = new TableEntity("partition", "int");
            intEntity.Add("int", 1);
            _tableClient.AddEntity(intEntity);

            // int64
            var longEntity = new TableEntity("partition", "long");
            longEntity.Add("long", 1L);
            _tableClient.AddEntity(longEntity);

            // double
            var doubleEntity = new TableEntity("partition", "double");
            doubleEntity.Add("double", 1.1);
            _tableClient.AddEntity(doubleEntity);

            // bool
            var boolEntity = new TableEntity("partition", "bool");
            boolEntity.Add("bool", true);
            _tableClient.AddEntity(boolEntity);

            // datetime
            var dateTimeEntity = new TableEntity("partition", "datetime");
            var dateTime = new DateTime(2020, 1, 1, 1, 1, 1, DateTimeKind.Utc);
            dateTimeEntity.Add("datetime", dateTime);
            _tableClient.AddEntity(dateTimeEntity);

            // datetimeoffset
            var dateTimeOffsetEntity = new TableEntity("partition", "datetimeoffset");
            var dateTimeOffset = new DateTimeOffset(2020, 1, 1, 1, 1, 1, TimeSpan.Zero);
            dateTimeOffsetEntity.Add("datetimeoffset", dateTimeOffset);
            _tableClient.AddEntity(dateTimeOffsetEntity);

            // binary
            var binaryEntity = new TableEntity("partition", "binary");
            var binary = Encoding.UTF8.GetBytes("binary");
            binaryEntity.Add("binary", binary);
            _tableClient.AddEntity(binaryEntity);

            // guid
            var guidEntity = new TableEntity("partition", "guid");
            var guid = Guid.NewGuid();
            guidEntity.Add("guid", guid);
            _tableClient.AddEntity(guidEntity);
        }

        [TestCleanup]
        public void Cleanup()
        {
            if (_tableClient is null)
                return;

            _tableClient.Delete();
        }
    }
}