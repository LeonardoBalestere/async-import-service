using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using ImportService.Contracts;
using ImportService.Data;
using ImportService.Gateway;
using ImportService.Messaging;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ImportDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddSingleton<IAmazonS3>(_ => new AmazonS3Client(
    new BasicAWSCredentials(builder.Configuration["S3:AccessKey"], builder.Configuration["S3:SecretKey"]),
    new AmazonS3Config
    {
        ServiceURL = builder.Configuration["S3:ServiceUrl"],
        // MinIO expõe buckets por path (localhost:9000/bucket), não por subdomínio.
        ForcePathStyle = true,
    }));

builder.Services.AddSingleton<IConnection>(_ =>
    new ConnectionFactory
    {
        HostName = builder.Configuration["RabbitMq:Host"]!,
        // guest/guest só aceita conexão de localhost; em cluster usamos outro usuário.
        UserName = builder.Configuration["RabbitMq:User"] ?? "guest",
        Password = builder.Configuration["RabbitMq:Password"] ?? "guest",
    }.CreateConnectionAsync().GetAwaiter().GetResult());

builder.Services.AddHostedService<OutboxDispatcher>();

var otlpEndpoint = new Uri(builder.Configuration["Otlp:Endpoint"] ?? "http://localhost:4317");

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("import-gateway"))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()          // cobre as chamadas S3 (o SDK usa HttpClient)
        .AddSource("Npgsql")                     // spans de SQL nativos do driver
        .AddSource("RabbitMQ.Client.Publisher")  // publish nativo do client 7 (injeta traceparent)
        .AddSource(ImportTelemetry.SourceName)
        .AddOtlpExporter(o => o.Endpoint = otlpEndpoint))
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddRuntimeInstrumentation()             // GC, heap, threads — a história da memória
        .AddMeter("Npgsql")
        .AddMeter(ImportTelemetry.SourceName)
        .AddOtlpExporter(o => o.Endpoint = otlpEndpoint));

builder.Logging.AddOpenTelemetry(logging =>
{
    logging.IncludeFormattedMessage = true;
    logging.IncludeScopes = true;
    logging.AddOtlpExporter(o => o.Endpoint = otlpEndpoint);
});

var app = builder.Build();

var bucket = app.Configuration["S3:Bucket"]!;

// Infra pronta antes de aceitar tráfego — as três operações são idempotentes.
using (var scope = app.Services.CreateScope())
{
    await scope.ServiceProvider.GetRequiredService<ImportDbContext>().Database.MigrateAsync();

    var s3 = scope.ServiceProvider.GetRequiredService<IAmazonS3>();
    try
    {
        await s3.PutBucketAsync(bucket);
    }
    catch (AmazonS3Exception e) when (e.ErrorCode is "BucketAlreadyOwnedByYou" or "BucketAlreadyExists")
    {
    }

    var mq = scope.ServiceProvider.GetRequiredService<IConnection>();
    await using var channel = await mq.CreateChannelAsync();
    await ImportsTopology.DeclareAsync(channel, app.Configuration.GetValue("RabbitMq:RetryDelayMs", 10000));
}

app.MapPost("/imports", async (IFormFile file, ImportDbContext db, IAmazonS3 s3, CancellationToken ct) =>
{
    // O upload passa por um arquivo temporário em disco — nunca inteiro pela memória.
    var tempPath = Path.GetTempFileName();
    try
    {
        await using (var temp = File.Create(tempPath))
        {
            await file.OpenReadStream().CopyToAsync(temp, ct);
        }

        string sha256;
        await using (var read = File.OpenRead(tempPath))
        {
            sha256 = Convert.ToHexString(await SHA256.HashDataAsync(read, ct)).ToLowerInvariant();
        }

        var existing = await db.ImportJobs.FirstOrDefaultAsync(j => j.FileSha256 == sha256, ct);
        if (existing is not null)
        {
            return Results.Ok(new { jobId = existing.Id, status = existing.Status.ToString(), duplicate = true });
        }

        var jobId = Guid.CreateVersion7();
        var job = new ImportJob
        {
            Id = jobId,
            FileName = file.FileName,
            FileSha256 = sha256,
            Bucket = bucket,
            ObjectKey = $"{jobId}/{file.FileName}",
            FileSizeBytes = file.Length,
            Status = ImportJobStatus.Received,
            CreatedAt = DateTimeOffset.UtcNow,
            UpdatedAt = DateTimeOffset.UtcNow,
        };

        await using (var read = File.OpenRead(tempPath))
        {
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = job.Bucket,
                Key = job.ObjectKey,
                InputStream = read,
                ContentType = file.ContentType,
            }, ct);
        }

        var message = new FileImportRequested
        {
            JobId = job.Id,
            Bucket = job.Bucket,
            ObjectKey = job.ObjectKey,
            FileName = job.FileName,
            ContentType = file.ContentType,
            FileSizeBytes = job.FileSizeBytes,
            FileSha256 = job.FileSha256,
            UploadedAt = job.CreatedAt,
        };

        // Outbox transacional: job e mensagem nascem na MESMA transação — ou os
        // dois existem, ou nenhum. Quem publica é o OutboxDispatcher, depois.
        db.ImportJobs.Add(job);
        db.OutboxMessages.Add(new OutboxMessage
        {
            Id = Guid.CreateVersion7(),
            Type = nameof(FileImportRequested),
            RoutingKey = ImportsTopology.XlsxRoutingKey,
            Payload = JsonSerializer.Serialize(message),
            CreatedAt = DateTimeOffset.UtcNow,
            // O trace do request atravessa a outbox: o dispatcher retoma este contexto.
            TraceParent = Activity.Current?.Id,
        });
        await db.SaveChangesAsync(ct);

        return Results.Accepted($"/imports/{job.Id}", new { jobId = job.Id, status = job.Status.ToString(), duplicate = false });
    }
    finally
    {
        File.Delete(tempPath);
    }
})
.DisableAntiforgery();

app.MapGet("/imports/{id:guid}", async (Guid id, ImportDbContext db, CancellationToken ct) =>
    await db.ImportJobs.FindAsync([id], ct) is { } job
        ? Results.Ok(new
        {
            jobId = job.Id,
            fileName = job.FileName,
            status = job.Status.ToString(),
            totalRows = job.TotalRows,
            error = job.ErrorMessage,
        })
        : Results.NotFound());

app.MapGet("/imports", async (ImportDbContext db, CancellationToken ct, int limit = 20) =>
{
    var jobs = await db.ImportJobs.AsNoTracking()
        .OrderByDescending(j => j.Id)
        .Take(Math.Clamp(limit, 1, 100))
        .ToListAsync(ct);

    return Results.Ok(jobs.Select(j => new
    {
        jobId = j.Id,
        fileName = j.FileName,
        status = j.Status.ToString(),
        totalRows = j.TotalRows,
        error = j.ErrorMessage,
    }));
});

app.Run();
