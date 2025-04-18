using Azure.Data.Tables;
using Azure.Data.Tables.Models;

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
    private TableClient _tableClient = null!;

    private const string createTableName = "createtesttable";
    const string createTableNameAsync = "testtableasync";


    private static string RandomTableName()
    {
        return "t" + Guid.NewGuid().ToString("N");
    }

    private void CreateTestData()
    {
        for (int i = 0; i < 30; i++)
        {
            List<TableTransactionAction> batch = [];
            for (int j = 0; j < 100; j++)
            {
                TableEntity e = new()
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

        List<TableEntity> rows = await _tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>("2");
        Assert.AreEqual(2, rows.Count);

        List<TableEntity> rows2 = await _tableClient.GetAllEntitiesByRowKeyAsync<TableEntity>(Guid.NewGuid().ToString());
        Assert.AreEqual(0, rows2.Count);
    }

    [TestMethod]
    public async Task GetEntitiesByPartitionKeyAsyncTest()
    {
        CreateTestData();

        List<TableEntity> rows1 = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("123");
        Assert.AreEqual(3000, rows1.Count);

        List<TableEntity> rows2 = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("2");
        Assert.AreEqual(1, rows2.Count);

        List<TableEntity> rows3 = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>(Guid.NewGuid().ToString());
        Assert.AreEqual(0, rows3.Count);
    }

    [TestMethod]
    public async Task GetFirstEntityAsyncTest()
    {
        CreateTestData();
        TableEntity? entity = await _tableClient.GetFirstEntityAsync<TableEntity>();
        Assert.IsNotNull(entity);
    }

    [TestMethod]
    public async Task GetFirstEntityByPartitionAsyncTest()
    {
        CreateTestData();
        TableEntity? entity = await _tableClient.GetFirstEntityAsync<TableEntity>("123");
        Assert.IsNotNull(entity);

        TableEntity? entity2 = await _tableClient.GetFirstEntityAsync<TableEntity>(Guid.NewGuid().ToString());
        Assert.IsNull(entity2);
    }

    [TestMethod]
    public async Task AddAllEntitiesAsyncTest()
    {
        List<TableEntity> entities = [];
        for (int i = 0; i < 1000; i++)
        {
            TableEntity e = new()
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
        List<TableEntity> remaining = await _tableClient.GetAllEntitiesByPartitionKeyAsync<TableEntity>("123");
        Assert.AreEqual(0, remaining.Count);
    }

    [TestMethod]
    public async Task CreateTableIfNotExistsSafeAsyncTest()
    {
        List<TableItem> tables = await _tableServiceClient.QueryAsync(x => x.Name == createTableNameAsync).ToListAsync();
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
        List<TableItem> tables = _tableServiceClient.Query(x => x.Name == createTableName).ToList();
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
        List<TableEntity> entites123 = await _tableClient.GetAllEntitiesStartingWithAsync<TableEntity>("PartitionKey", "123");
        Assert.AreEqual(3000, entites123.Count);

        List<TableEntity> entites1 = await _tableClient.GetAllEntitiesStartingWithAsync<TableEntity>("PartitionKey", "1");
        Assert.AreEqual(3001, entites1.Count);

        List<TableEntity> entities2 = await _tableClient.GetAllEntitiesStartingWithAsync<TableEntity>("PartitionKey", "2");
        Assert.AreEqual(1, entities2.Count);
    }


    [TestMethod]
    public async Task TestEntitiesCount()
    {
        CreateTestData();
        int count = await _tableClient.CountEntitiesAsync();
        Assert.AreEqual(3003, count);

        int count2 = await _tableClient.CountEntitiesAsync(partitionKey: "123");
        Assert.AreEqual(3000, count2);
    }

    [TestCleanup]
    public void Cleanup()
    {
        _tableClient?.Delete();
        _tableServiceClient.DeleteTable(createTableName);
        _tableServiceClient.DeleteTable(createTableNameAsync);
    }
}
