using Azure.Data.Tables;
using Microsoft.VisualStudio.TestTools.UnitTesting;
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
        //private const string ConnectionString = $"DefaultEndpointsProtocol={DefaultEndpointsProtocol};AccountName={AccountName};AccountKey={AccountKey};TableEndpoint={TableEndpoint};";
        private const string ConnectionString = "DefaultEndpointsProtocol=https;AccountName=tablestoragetest;AccountKey=YAYsqjyQGwO2ngoBEhiifgSc0JNgQLToVOidIcELAUaVS1EUN4iIMowIZCCcr2VTQRunB3huRjjKK1sFHrDswg==;EndpointSuffix=core.windows.net";
        private TableServiceClient? _tableServiceClient;
        private TableClient? _tableClient;

        private const string createTableName = "createtesttable";
        const string createTableNameAsync = "testtableasync";

        [TestInitialize]
        public void Initialize()
        {
            _tableServiceClient = new TableServiceClient(ConnectionString);
            _tableClient = _tableServiceClient.GetTableClient(RandomTableName());
            _tableClient.CreateIfNotExists();
        }

        [TestMethod]
        public async Task TestExport()
        {
            CreateTestData();
            await _tableClient.ExportAsCSV();
            Assert.IsTrue(true);
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
            stringEntity.Add("string", "string");
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