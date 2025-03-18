using Azure.Data.Tables;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Medienstudio.Azure.Data.Tables.Extensions.Tests;

[TestClass]
public class ExtensionTests
{
    private const string DefaultEndpointsProtocol = "http";
    private const string AccountName = "devstoreaccount1";
    private const string AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private const string TableEndpoint = "http://127.0.0.1:10002/devstoreaccount1";
    private const string ConnectionString = $"DefaultEndpointsProtocol={DefaultEndpointsProtocol};AccountName={AccountName};AccountKey={AccountKey};TableEndpoint={TableEndpoint};";
    private readonly TableServiceClient _tableServiceClient = new(ConnectionString);
    private TableClient? _tableClient;

    private const string createTableName = "createtesttable";
    const string createTableNameAsync = "testtableasync";


    private static string RandomTableName()
    {
        return "t" + Guid.NewGuid().ToString("N");
    }

    private void CreateTestData()
    {
        if (_tableClient is null)
            return;

        for(int i = 0; i < 30; i++)
        {
            List<TableTransactionAction> batch = new();
            for (int j = 0; j < 100; j++)
            {
                TableEntity e = new TableEntity()
                {
                    PartitionKey = "123",
                    RowKey = Guid.NewGuid().ToString()
                };
                e.Add("Test", Guid.NewGuid().ToString());
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, e));
            }
            _tableClient.SubmitTransaction(batch);
        }

        _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "1", RowKey = "2" });
        _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "2", RowKey = "2" });
        _tableClient.UpsertEntity(new TableEntity() { PartitionKey = "3", RowKey = "4" });
    }

    [TestInitialize]
    public void Initialize()
    {
        _tableClient = _tableServiceClient.GetTableClient(RandomTableName());
        _tableClient.CreateIfNotExists();
    }

    [TestMethod]
    public async Task GetAllEntitiesAsyncTest()
    {
        CreateTestData();

        List<TableEntity> rows = await _tableClient.GetAllEntitiesAsync<TableEntity>();
        Assert.AreEqual(3003, rows.Count);
    }

    [TestMethod]
    public async Task GetEntitiesByRowKeyAsyncTest()
    {
        CreateTestData();

        IList<TableEntity> rows = await _tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>("2");
        Assert.AreEqual(2, rows.Count);

        IList<TableEntity> rows2 = await _tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>(Guid.NewGuid().ToString());
        Assert.AreEqual(0, rows2.Count);
    }

    [TestMethod]
    public async Task GetEntitiesByPartitionKeyAsyncTest()
    {
        CreateTestData();

        IList<TableEntity> rows1 = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("123");
        Assert.AreEqual(3000, rows1.Count);

        IList<TableEntity> rows2 = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("2");
        Assert.AreEqual(1, rows2.Count);

        IList<TableEntity> rows3 = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>(Guid.NewGuid().ToString());
        Assert.AreEqual(0, rows3.Count);
    }

    [TestMethod]
    public async Task GetFirstEntityAsyncTest()
    {
        CreateTestData();
        TableEntity entity = await _tableClient.GetFirstEntityAsync<TableEntity>();
        Assert.IsNotNull(entity);
    }

    [TestMethod]
    public async Task GetFirstEntityByPartitionAsyncTest()
    {
        CreateTestData();
        TableEntity entity = await _tableClient.GetFirstEntityAsync<TableEntity>("123");
        Assert.IsNotNull(entity);

        TableEntity entity2 = await _tableClient.GetFirstEntityAsync<TableEntity>(Guid.NewGuid().ToString());
        Assert.IsNull(entity2);
    }

    [TestMethod]
    public async Task AddAllEntitiesAsyncTest()
    {
        List<TableEntity> entities = new();
        for (int i = 0; i < 1000; i++)
        {
            TableEntity e = new TableEntity()
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
        List<TableEntity> remaining = await _tableClient.GetAllEntitiesAsync<TableEntity>();
        Assert.AreEqual(0, remaining.Count);
    }

    [TestMethod]
    public async Task DeleteAllEntitiesByPartitonKeyAsyncTest()
    {
        CreateTestData();
        await _tableClient.DeleteAllEntitiesByPartitionKeyAsync("123");
        IList<TableEntity> remaining = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("123");
        Assert.AreEqual(0, remaining.Count);
    }

    [TestMethod]
    public async Task CreateTableIfNotExistsSafeAsyncTest()
    {
        List<global::Azure.Data.Tables.Models.TableItem> tables = await _tableServiceClient.QueryAsync(x => x.Name == createTableNameAsync).ToListAsync();
        Assert.AreEqual(0, tables.Count);

        await _tableServiceClient.CreateTableIfNotExistsSafeAsync(createTableNameAsync);
        await _tableServiceClient.CreateTableIfNotExistsSafeAsync(createTableNameAsync);

        tables = await _tableServiceClient.QueryAsync(x => x.Name == createTableNameAsync).ToListAsync();
        Assert.AreEqual(1, tables.Count);

        _tableServiceClient.DeleteTable(createTableNameAsync);
    }

    [TestMethod]
    public void CreateTableIfNotExistsSafeTest()
    {
        List<global::Azure.Data.Tables.Models.TableItem> tables = _tableServiceClient.Query(x => x.Name == createTableName).ToList();
        Assert.AreEqual(0, tables.Count);

        _tableServiceClient.CreateTableIfNotExistsSafe(createTableName);
        _tableServiceClient.CreateTableIfNotExistsSafe(createTableName);

        tables = _tableServiceClient.Query(x => x.Name == createTableName).ToList();
        Assert.AreEqual(1, tables.Count);

        _tableServiceClient.DeleteTable(createTableName);
    }

    [TestMethod]
    public async Task GetAllEntitiesStartingWithTest()
    {
        CreateTestData();
        IList<TableEntity> entites123 = await _tableClient.GetAllEntitiesStartingWithAsync<TableEntity>("PartitionKey", "123");
        Assert.AreEqual(3000, entites123.Count);

        IList<TableEntity> entites1 = await _tableClient.GetAllEntitiesStartingWithAsync<TableEntity>("PartitionKey", "1");
        Assert.AreEqual(3001, entites1.Count);

        IList<TableEntity> entities2 = await _tableClient.GetAllEntitiesStartingWithAsync<TableEntity>("PartitionKey", "2");
        Assert.AreEqual(1, entities2.Count);
    }

    [TestCleanup]
    public void Cleanup()
    {
        _tableClient?.Delete();
        _tableServiceClient.DeleteTable(createTableName);
        _tableServiceClient.DeleteTable(createTableNameAsync);
    }
}
