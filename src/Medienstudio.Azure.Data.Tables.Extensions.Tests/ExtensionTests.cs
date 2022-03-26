using Azure.Data.Tables;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
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
        private TableServiceClient? _tableServiceClient;
        private TableClient? _tableClient;

        private static string RandomTableName()
        {
            return "t" + Guid.NewGuid().ToString("N");
        }

        private void CreateTestData()
        {
            if (_tableClient is null)
                return;

            List<TableTransactionAction> batch = new();

            for (int i = 0; i < 100; i++)
            {
                var e = new TableEntity()
                {
                    PartitionKey = "123",
                    RowKey = Guid.NewGuid().ToString()
                };
                e.Add("Test", Guid.NewGuid().ToString());
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, e));
            }

            _tableClient.SubmitTransaction(batch);

            _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "1", RowKey = "2" });
            _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "2", RowKey = "2" });
            _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "3", RowKey = "4" });
        }

        [TestInitialize]
        public void Initialize()
        {
            _tableServiceClient = new TableServiceClient(ConnectionString);
            _tableClient = _tableServiceClient.GetTableClient(RandomTableName());
            _tableClient.CreateIfNotExists();
        }

        [TestMethod]
        public async Task GetAllEntitiesAsyncTest()
        {
            CreateTestData();

            var rows = await _tableClient.GetAllEntitiesAsync<TableEntity>();
            Assert.AreEqual(103, rows.Count);
        }

        [TestMethod]
        public async Task GetEntitiesByRowKeyAsyncTest()
        {
            CreateTestData();

            var rows = await _tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>("2");
            Assert.AreEqual(1, rows.Count);

            var rows2 = await _tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>(Guid.NewGuid().ToString());
            Assert.AreEqual(0, rows2.Count);
        }

        [TestMethod]
        public async Task GetEntitiesByPartitionKeyAsyncTest()
        {
            CreateTestData();

            var rows = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("2");
            Assert.AreEqual(2, rows.Count);

            var rows2 = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>(Guid.NewGuid().ToString());
            Assert.AreEqual(0, rows2.Count);
        }

        [TestMethod]
        public async Task AddAllEntitiesAsyncTest()
        {
            List<TableEntity> entities = new();
            for (int i = 0; i < 1000; i++)
            {
                var e = new TableEntity()
                {
                    PartitionKey = (i % 20).ToString(),
                    RowKey = Guid.NewGuid().ToString()
                };
                entities.Add(e);
            }
            await _tableClient.AddEntitiesAsync(entities);
        }

        [TestMethod]
        public async Task DeleteAllEntitiesAsyncTest()
        {
            CreateTestData();
            await _tableClient.DeleteAllEntitiesAsync();
            var remaining = await _tableClient.GetAllEntitiesAsync<TableEntity>();
            Assert.AreEqual(0, remaining.Count);
        }

        [TestMethod]
        public async Task CreateTableIfNotExistsSafeTest()
        {
            const string tableName = "testtable";

            var tables = await _tableServiceClient.QueryAsync(x => x.Name == tableName).ToListAsync();
            Assert.AreEqual(0, tables.Count);

            await _tableServiceClient.CreateTableIfNotExistsSafeAsync(tableName);
            await _tableServiceClient.CreateTableIfNotExistsSafeAsync(tableName);

            tables = await _tableServiceClient.QueryAsync(x => x.Name == tableName).ToListAsync();
            Assert.AreEqual(1, tables.Count);
        }


        [TestCleanup]
        public void Cleanup()
        {
            _tableClient?.Delete();
        }
    }
}