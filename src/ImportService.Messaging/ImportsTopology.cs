using RabbitMQ.Client;

namespace ImportService.Messaging;

/// <summary>
/// Topologia de mensageria — parte do CONTRATO entre os serviços: os argumentos
/// de uma fila são imutáveis (redeclarar com args diferentes dá PRECONDITION_FAILED),
/// então Gateway e Worker precisam declarar exatamente a mesma coisa.
///
/// Fluxo de falha: nack(requeue=false) na fila principal → DLX → fila de retry
/// (espera o TTL) → DLX de volta pra principal. Após esgotar as tentativas
/// (contadas pelo header x-death), o worker publica a mensagem na DLQ (parking lot).
/// </summary>
public static class ImportsTopology
{
    public const string Exchange = "imports";
    public const string RetryExchange = "imports.retry";
    public const string DlqExchange = "imports.dlq";

    public const string FileImportsQueue = "file-imports";
    public const string RetryQueue = "file-imports.retry";
    public const string DlqQueue = "file-imports.dlq";

    /// <summary>Roteamento por tipo de arquivo: um futuro "file.csv" ganha fila própria sem tocar nesta.</summary>
    public const string XlsxRoutingKey = "file.xlsx";

    public static async Task DeclareAsync(IChannel channel, int retryDelayMs, CancellationToken ct = default)
    {
        await channel.ExchangeDeclareAsync(Exchange, ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: ct);
        await channel.ExchangeDeclareAsync(RetryExchange, ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: ct);
        await channel.ExchangeDeclareAsync(DlqExchange, ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: ct);

        await channel.QueueDeclareAsync(FileImportsQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-dead-letter-exchange"] = RetryExchange },
            cancellationToken: ct);
        await channel.QueueBindAsync(FileImportsQueue, Exchange, XlsxRoutingKey, cancellationToken: ct);

        await channel.QueueDeclareAsync(RetryQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-message-ttl"] = retryDelayMs,
                ["x-dead-letter-exchange"] = Exchange,
            },
            cancellationToken: ct);
        await channel.QueueBindAsync(RetryQueue, RetryExchange, XlsxRoutingKey, cancellationToken: ct);

        await channel.QueueDeclareAsync(DlqQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.QueueBindAsync(DlqQueue, DlqExchange, XlsxRoutingKey, cancellationToken: ct);
    }
}
