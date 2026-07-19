using System.Security.Cryptography;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using ImportService.Contracts;
using ImportService.Data;
using Microsoft.EntityFrameworkCore;
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
    new ConnectionFactory { HostName = builder.Configuration["RabbitMq:Host"]! }
        .CreateConnectionAsync().GetAwaiter().GetResult());

var app = builder.Build();

var bucket = app.Configuration["S3:Bucket"]!;
var queue = app.Configuration["RabbitMq:Queue"]!;

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
    await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
}

app.MapPost("/imports", async (IFormFile file, ImportDbContext db, IAmazonS3 s3, IConnection mq, CancellationToken ct) =>
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

        db.ImportJobs.Add(job);
        await db.SaveChangesAsync(ct);

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

        await using (var channel = await mq.CreateChannelAsync(cancellationToken: ct))
        {
            await channel.BasicPublishAsync(
                // Exchange default: a routing key é o próprio nome da fila.
                exchange: string.Empty,
                routingKey: queue,
                mandatory: false,
                basicProperties: new BasicProperties { Persistent = true, ContentType = "application/json" },
                body: JsonSerializer.SerializeToUtf8Bytes(message),
                cancellationToken: ct);
        }

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

app.Run();
