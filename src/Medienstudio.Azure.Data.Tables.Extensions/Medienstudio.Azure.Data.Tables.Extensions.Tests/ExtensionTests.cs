using Azure.Data.Tables;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;

namespace Medienstudio.Azure.Data.Tables.Extensions.Tests
{
    [TestClass]
    public class ExtensionTests
    {
        private const string DefaultEndpointsProtocol = "http";
        private const string AccountName = "devstoreaccount1";
        private const string AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        private const string TableEndpoint = "http://127.0.0.1:10002/devstoreaccount1";
        private const string ConnectionString = $"DefaultEndpointsProtocol={DefaultEndpointsProtocol};AccountName={AccountName};AccountKey={AccountKey};TableEndpoint={TableEndpoint};";
        
        private TableClient _tableClient = new(connectionString: ConnectionString, tableName: "TestTable");

        [TestInitialize]
        public void Initialize()
        {
            _tableClient.CreateIfNotExists();
            _tableClient.UpsertEntity(new TableEntity() { PartitionKey= "1", RowKey= "2" });
            _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "2", RowKey = "2" });
            _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "3", RowKey = "4" });

        }

        [TestMethod]
        public async Task GetByRowKeyTest()
        {
            var rows = await _tableClient.GetByRowKeyAsync<TableEntity>("2");
            Assert.AreEqual(2, rows.Count);

            var rows2 = await _tableClient.GetByRowKeyAsync<TableEntity>(Guid.NewGuid().ToString());
            Assert.AreEqual(0, rows2.Count);
        }
    }
}