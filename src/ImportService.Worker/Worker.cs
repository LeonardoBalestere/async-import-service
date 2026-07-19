using System.Text.Json;
using Amazon.S3;
using ImportService.Contracts;
using ImportService.Data;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ImportService.Worker;

public class Worker(
    IConnection connection,
    IAmazonS3 s3,
    ExcelTransactionParser parser,
    IServiceScopeFactory scopeFactory,
    IConfiguration configuration,
    ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = configuration["RabbitMq:Queue"]!;

        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
            cancellationToken: stoppingToken);

        // Uma mensagem não-ackada por vez: um arquivo grande já é trabalho suficiente,
        // e mensagens não pré-buscadas ficam livres para outros workers concorrentes.
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            var message = JsonSerializer.Deserialize<FileImportRequested>(delivery.Body.Span)!;
            await ProcessAsync(message, stoppingToken);

            // Ack manual só depois de persistir o resultado: se o worker morrer antes
            // desta linha, o broker reentrega a mensagem a outro worker.
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false,
                cancellationToken: stoppingToken);
        };

        await channel.BasicConsumeAsync(queue, autoAck: false, consumer,
            cancellationToken: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task ProcessAsync(FileImportRequested message, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ImportDbContext>();

        var job = await db.ImportJobs.FindAsync([message.JobId], ct);
        if (job is null)
        {
            logger.LogWarning("Job {JobId} não existe no banco; descartando mensagem", message.JobId);
            return;
        }

        if (job.Status == ImportJobStatus.Completed)
        {
            logger.LogInformation("Job {JobId} já concluído (mensagem reentregue); ignorando", message.JobId);
            return;
        }

        job.Status = ImportJobStatus.Processing;
        job.UpdatedAt = DateTimeOffset.UtcNow;
        await db.SaveChangesAsync(ct);

        try
        {
            // Fase 1: download e parse ingênuos DE PROPÓSITO — o arquivo inteiro vai
            // para a memória, reproduzindo o bug que motivou este projeto.
            // A Fase 2 substitui por leitura em streaming e mede a diferença.
            using var response = await s3.GetObjectAsync(message.Bucket, message.ObjectKey, ct);
            using var buffer = new MemoryStream();
            await response.ResponseStream.CopyToAsync(buffer, ct);
            buffer.Position = 0;

            var rows = parser.Parse(buffer);

            // Reprocessar do zero é idempotente: a transação apaga o que uma tentativa
            // interrompida tenha deixado e regrava tudo, ou nada acontece.
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            await db.ImportedTransactions.Where(t => t.JobId == job.Id).ExecuteDeleteAsync(ct);
            db.ImportedTransactions.AddRange(rows.Select(r => new ImportedTransaction
            {
                JobId = job.Id,
                RowNumber = r.RowNumber,
                Date = r.Date,
                Account = r.Account,
                Description = r.Description,
                Amount = r.Amount,
            }));
            job.Status = ImportJobStatus.Completed;
            job.TotalRows = rows.Count;
            job.ErrorMessage = null;
            job.UpdatedAt = DateTimeOffset.UtcNow;
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);

            logger.LogInformation("Job {JobId} concluído: {Rows} linhas importadas", job.Id, rows.Count);
        }
        catch (Exception ex)
        {
            // Sem DLQ ainda (Fase 2): a falha vira estado no ledger e a mensagem é ackada.
            job.Status = ImportJobStatus.Failed;
            job.ErrorMessage = ex.Message;
            job.UpdatedAt = DateTimeOffset.UtcNow;
            await db.SaveChangesAsync(CancellationToken.None);
            logger.LogError(ex, "Job {JobId} falhou", job.Id);
        }
    }
}
