using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using ImportService.Contracts;
using ImportService.Data;
using ImportService.Messaging;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ImportService.Worker;

public class Worker(
    IConnection connection,
    IAmazonS3 s3,
    ITransactionParser parser,
    IServiceScopeFactory scopeFactory,
    IConfiguration configuration,
    ILogger<Worker> logger) : BackgroundService
{
    private const int BatchSize = 5000;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var maxAttempts = configuration.GetValue("RabbitMq:MaxAttempts", 3);

        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await ImportsTopology.DeclareAsync(channel,
            configuration.GetValue("RabbitMq:RetryDelayMs", 10000), stoppingToken);

        // Uma mensagem não-ackada por vez: um arquivo grande já é trabalho
        // suficiente, e as demais ficam livres para workers concorrentes.
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            FileImportRequested? message = null;
            try
            {
                message = JsonSerializer.Deserialize<FileImportRequested>(delivery.Body.Span);
                await ProcessAsync(message!, ExtractTraceContext(delivery.BasicProperties), stoppingToken);

                // Ack manual só depois de persistir: worker morto antes desta linha
                // significa reentrega — e reentrega é segura (processamento idempotente).
                await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false,
                    cancellationToken: stoppingToken);
            }
            catch (Exception ex)
            {
                // É o broker quem carrega o número da tentativa: x-death conta as
                // rejeições desta mensagem na fila principal.
                var attempt = GetRejectionCount(delivery.BasicProperties) + 1;
                var exhausted = message is null || attempt >= maxAttempts;

                await RecordFailureAsync(message, ex, exhausted, attempt, stoppingToken);

                if (exhausted)
                {
                    // Parking-lot: cópia explícita para a DLQ e ack do original.
                    await channel.BasicPublishAsync(ImportsTopology.DlqExchange, delivery.RoutingKey,
                        mandatory: false, basicProperties: new BasicProperties(delivery.BasicProperties),
                        body: delivery.Body, cancellationToken: stoppingToken);
                    await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false,
                        cancellationToken: stoppingToken);
                    logger.LogError(ex, "Job {JobId} enviado à DLQ após {Attempt} tentativa(s)",
                        message?.JobId, attempt);
                }
                else
                {
                    // Nack sem requeue → DLX → fila de retry → TTL → volta pra principal.
                    await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false,
                        cancellationToken: stoppingToken);
                    logger.LogWarning(ex, "Tentativa {Attempt} do job {JobId} falhou; retry agendado",
                        attempt, message?.JobId);
                }
            }
        };

        await channel.BasicConsumeAsync(ImportsTopology.FileImportsQueue, autoAck: false, consumer,
            cancellationToken: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task ProcessAsync(FileImportRequested message, ActivityContext parentContext, CancellationToken ct)
    {
        // Propagação manual: o pai vem do header W3C "traceparent" que o publish
        // injetou na mensagem — o trace continua o do request original.
        using var activity = ImportTelemetry.ActivitySource.StartActivity(
            "import process", ActivityKind.Consumer, parentContext);
        activity?.SetTag("import.job_id", message.JobId);
        activity?.SetTag("import.file_size_bytes", message.FileSizeBytes);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            using var scope = scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<ImportDbContext>();

            var job = await db.ImportJobs.FindAsync([message.JobId], ct)
                ?? throw new InvalidOperationException($"Job {message.JobId} não existe no banco");

            if (job.Status == ImportJobStatus.Completed)
            {
                logger.LogInformation("Job {JobId} já concluído (reentrega); ignorando", job.Id);
                return;
            }

            job.Status = ImportJobStatus.Processing;
            job.UpdatedAt = DateTimeOffset.UtcNow;
            await db.SaveChangesAsync(ct);

            // Download para arquivo temporário: o leitor precisa de Seek (xlsx é um ZIP
            // com o diretório central no FIM) e o peso do arquivo fica em disco, não na RAM.
            var tempPath = Path.GetTempFileName();
            try
            {
                using (var response = await s3.GetObjectAsync(message.Bucket, message.ObjectKey, ct))
                await using (var temp = File.Create(tempPath))
                {
                    await response.ResponseStream.CopyToAsync(temp, ct);
                }

                var total = 0;
                await using (var fileStream = File.OpenRead(tempPath))
                await using (var tx = await db.Database.BeginTransactionAsync(ct))
                {
                    await db.ImportedTransactions.Where(t => t.JobId == job.Id).ExecuteDeleteAsync(ct);

                    // Inserts em lote com ChangeTracker.Clear(): a memória do EF fica
                    // limitada ao lote corrente, não ao arquivo inteiro.
                    var batch = new List<ImportedTransaction>(BatchSize);
                    foreach (var row in parser.Parse(fileStream))
                    {
                        batch.Add(new ImportedTransaction
                        {
                            JobId = job.Id,
                            RowNumber = row.RowNumber,
                            Date = row.Date,
                            Account = row.Account,
                            Description = row.Description,
                            Amount = row.Amount,
                        });

                        if (batch.Count == BatchSize)
                        {
                            total += await FlushAsync(db, batch, ct);
                        }
                    }

                    total += await FlushAsync(db, batch, ct);

                    // ChangeTracker.Clear() desanexou o job — ExecuteUpdate escreve direto.
                    await db.ImportJobs.Where(j => j.Id == job.Id).ExecuteUpdateAsync(set => set
                        .SetProperty(j => j.Status, ImportJobStatus.Completed)
                        .SetProperty(j => j.TotalRows, total)
                        .SetProperty(j => j.ErrorMessage, (string?)null)
                        .SetProperty(j => j.UpdatedAt, DateTimeOffset.UtcNow), ct);

                    await tx.CommitAsync(ct);
                }

                activity?.SetTag("import.total_rows", total);
                ImportTelemetry.RowsImported.Add(total);
                ImportTelemetry.ImportDuration.Record(stopwatch.Elapsed.TotalSeconds);

                logger.LogInformation("Job {JobId} concluído: {Rows} linhas importadas", job.Id, total);
            }
            finally
            {
                File.Delete(tempPath);
            }
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    private static async Task<int> FlushAsync(ImportDbContext db, List<ImportedTransaction> batch, CancellationToken ct)
    {
        if (batch.Count == 0)
        {
            return 0;
        }

        db.ImportedTransactions.AddRange(batch);
        await db.SaveChangesAsync(ct);
        db.ChangeTracker.Clear();

        var flushed = batch.Count;
        batch.Clear();
        return flushed;
    }

    private async Task RecordFailureAsync(FileImportRequested? message, Exception ex, bool exhausted,
        long attempt, CancellationToken ct)
    {
        if (message is null)
        {
            return; // mensagem indeserializável: não há job para atualizar
        }

        try
        {
            using var scope = scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<ImportDbContext>();
            var status = exhausted ? ImportJobStatus.Failed : ImportJobStatus.Retrying;
            await db.ImportJobs.Where(j => j.Id == message.JobId).ExecuteUpdateAsync(set => set
                .SetProperty(j => j.Status, status)
                .SetProperty(j => j.ErrorMessage, $"Tentativa {attempt}: {ex.Message}")
                .SetProperty(j => j.UpdatedAt, DateTimeOffset.UtcNow), ct);
        }
        catch (Exception dbEx)
        {
            logger.LogError(dbEx, "Não foi possível registrar a falha do job {JobId}", message.JobId);
        }
    }

    private static ActivityContext ExtractTraceContext(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is not null
            && properties.Headers.TryGetValue("traceparent", out var raw)
            && raw is byte[] bytes
            && ActivityContext.TryParse(Encoding.UTF8.GetString(bytes), null, out var context))
        {
            return context;
        }

        return default;
    }

    private static long GetRejectionCount(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null
            || !properties.Headers.TryGetValue("x-death", out var raw)
            || raw is not IList<object> deaths)
        {
            return 0;
        }

        foreach (var entry in deaths)
        {
            if (entry is IDictionary<string, object> death
                && death.TryGetValue("queue", out var queue)
                && queue is byte[] queueBytes
                && Encoding.UTF8.GetString(queueBytes) == ImportsTopology.FileImportsQueue
                && death.TryGetValue("count", out var count))
            {
                return Convert.ToInt64(count);
            }
        }

        return 0;
    }
}
