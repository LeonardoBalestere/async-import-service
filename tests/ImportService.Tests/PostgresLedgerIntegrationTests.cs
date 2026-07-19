using ImportService.Data;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;

namespace ImportService.Tests;

/// <summary>Um Postgres real por classe de teste, descartado ao final.</summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    public PostgreSqlContainer Container { get; } = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public Task InitializeAsync() => Container.StartAsync();

    public Task DisposeAsync() => Container.DisposeAsync().AsTask();
}

public class PostgresLedgerIntegrationTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    private ImportDbContext CreateContext() => new(new DbContextOptionsBuilder<ImportDbContext>()
        .UseNpgsql(fixture.Container.GetConnectionString())
        .Options);

    private static ImportJob Job(string hash) => new()
    {
        Id = Guid.CreateVersion7(),
        FileName = "lancamentos.xlsx",
        FileSha256 = hash,
        Bucket = "imports",
        ObjectKey = "k",
        Status = ImportJobStatus.Received,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow,
    };

    [Fact]
    public async Task Migrations_aplicam_e_o_hash_unico_rejeita_upload_duplicado()
    {
        await using (var db = CreateContext())
        {
            await db.Database.MigrateAsync();
            db.ImportJobs.Add(Job("mesmo-hash"));
            await db.SaveChangesAsync();
        }

        // A garantia de dedupe não é o if do Gateway — é a unique constraint:
        // duas réplicas passando pela checagem ao mesmo tempo, uma delas falha aqui.
        await using (var db = CreateContext())
        {
            db.ImportJobs.Add(Job("mesmo-hash"));
            await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync());
        }
    }
}
