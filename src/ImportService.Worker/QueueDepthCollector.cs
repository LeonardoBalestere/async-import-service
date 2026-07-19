using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using ImportService.Messaging;
using RabbitMQ.Client;

namespace ImportService.Worker;

/// <summary>
/// Consulta o broker a cada 5s e expõe a profundidade das filas como
/// ObservableGauge — a métrica que o KEDA usará como sinal de escala na Fase 4.
/// Caveat conhecido: a métrica vive no worker; worker morto = métrica ausente
/// (em produção, o plugin rabbitmq_prometheus cobriria esse ponto cego).
/// </summary>
public class QueueDepthCollector : BackgroundService
{
    private static readonly string[] Queues =
    [
        ImportsTopology.FileImportsQueue,
        ImportsTopology.RetryQueue,
        ImportsTopology.DlqQueue,
    ];

    private readonly ConcurrentDictionary<string, long> _depths = new();
    private readonly IConnection _connection;
    private readonly ILogger<QueueDepthCollector> _logger;

    public QueueDepthCollector(IConnection connection, ILogger<QueueDepthCollector> logger)
    {
        _connection = connection;
        _logger = logger;

        ImportTelemetry.Meter.CreateObservableGauge(
            "rabbitmq.queue.depth",
            () => _depths.Select(d => new Measurement<long>(
                d.Value, new KeyValuePair<string, object?>("queue", d.Key))),
            unit: "{messages}",
            description: "Mensagens prontas na fila");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);
                foreach (var queue in Queues)
                {
                    _depths[queue] = await channel.MessageCountAsync(queue, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Falha ao coletar profundidade das filas");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}
