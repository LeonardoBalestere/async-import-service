using Amazon.DynamoDBv2;
using Amazon.Runtime;
using ImportService.Data;
using Testcontainers.LocalStack;

namespace ImportService.Tests;

/// <summary>Um LocalStack real (DynamoDB) por classe de teste.</summary>
public sealed class LocalStackFixture : IAsyncLifetime
{
    private readonly LocalStackContainer _container = new LocalStackBuilder("localstack/localstack:4").Build();

    public IAmazonDynamoDB Client { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        Client = new AmazonDynamoDBClient(
            new BasicAWSCredentials("test", "test"),
            new AmazonDynamoDBConfig
            {
                ServiceURL = _container.GetConnectionString(),
                AuthenticationRegion = "us-east-1",
            });
    }

    public async Task DisposeAsync()
    {
        Client?.Dispose();
        await _container.DisposeAsync();
    }
}

public class JobStatusStoreIntegrationTests(LocalStackFixture fixture) : IClassFixture<LocalStackFixture>
{
    [Fact]
    public async Task Latest_e_timeline_refletem_as_transicoes_na_ordem()
    {
        var store = new JobStatusStore(fixture.Client, "import-job-status", ttlSeconds: 300);
        await store.EnsureTableAsync();

        var jobId = Guid.CreateVersion7();
        await store.WriteTransitionAsync(jobId, "Received");
        await store.WriteTransitionAsync(jobId, "Completed", totalRows: 42);

        var latest = await store.GetLatestAsync(jobId);
        Assert.NotNull(latest);
        Assert.Equal("Completed", latest.Status);
        Assert.Equal(42, latest.TotalRows);

        var timeline = await store.GetTimelineAsync(jobId);
        Assert.Equal(["Received", "Completed"], timeline.Select(t => t.Status).ToArray());
    }

    [Fact]
    public async Task Item_expirado_fica_invisivel_na_leitura_mesmo_fisicamente_presente()
    {
        // TTL negativo: o item nasce expirado — presente no storage, invisível na API.
        var store = new JobStatusStore(fixture.Client, "import-job-status", ttlSeconds: -5);
        await store.EnsureTableAsync();

        var jobId = Guid.CreateVersion7();
        await store.WriteTransitionAsync(jobId, "Completed");

        Assert.Null(await store.GetLatestAsync(jobId));
        Assert.Empty(await store.GetTimelineAsync(jobId));
    }
}
