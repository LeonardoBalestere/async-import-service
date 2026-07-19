using System.Text;
using ImportService.Data;
using ImportService.Messaging;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;

namespace ImportService.Gateway;

/// <summary>
/// Publica as mensagens pendentes da outbox no broker. A linha só é marcada como
/// despachada depois que o broker CONFIRMA a gravação (publisher confirms) —
/// se o processo morrer entre o publish e o commit da marcação, a mensagem é
/// republicada no próximo ciclo (at-least-once).
/// </summary>
public class OutboxDispatcher(
    IServiceScopeFactory scopeFactory,
    IConnection connection,
    IConfiguration configuration,
    ILogger<OutboxDispatcher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromMilliseconds(configuration.GetValue("Outbox:PollIntervalMs", 500));

        while (!stoppingToken.IsCancellationRequested)
        {
            var dispatched = 0;
            try
            {
                dispatched = await DispatchPendingAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Ciclo do outbox falhou; nova tentativa em {Interval}", interval);
            }

            if (dispatched == 0)
            {
                try
                {
                    await Task.Delay(interval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    private async Task<int> DispatchPendingAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ImportDbContext>();

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        // SKIP LOCKED: réplicas concorrentes do gateway pegam lotes disjuntos
        // em vez de bloquear (ou duplicar) umas às outras.
        var batch = await db.OutboxMessages
            .FromSqlRaw("""
                SELECT * FROM "OutboxMessages"
                WHERE "DispatchedAt" IS NULL
                ORDER BY "CreatedAt"
                LIMIT 50
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(ct);

        if (batch.Count == 0)
        {
            return 0;
        }

        var confirmed = 0;
        await using var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            ct);

        foreach (var message in batch)
        {
            try
            {
                // Com confirms ligado, este await só completa quando o broker confirma.
                await channel.BasicPublishAsync(
                    ImportsTopology.Exchange,
                    message.RoutingKey,
                    mandatory: false,
                    basicProperties: new BasicProperties
                    {
                        Persistent = true,
                        ContentType = "application/json",
                        MessageId = message.Id.ToString(),
                        Type = message.Type,
                    },
                    body: Encoding.UTF8.GetBytes(message.Payload),
                    cancellationToken: ct);

                message.DispatchedAt = DateTimeOffset.UtcNow;
                confirmed++;
            }
            catch (Exception ex)
            {
                message.Attempts++;
                message.LastError = ex.Message;
                logger.LogWarning(ex, "Publish da outbox {MessageId} falhou (tentativa {Attempts})",
                    message.Id, message.Attempts);
                break; // broker provavelmente fora; o resto do lote espera o próximo ciclo
            }
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return confirmed;
    }
}
