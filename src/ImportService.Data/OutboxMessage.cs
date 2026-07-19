namespace ImportService.Data;

/// <summary>
/// Outbox transacional: a mensagem nasce na MESMA transação do dado que a origina.
/// O OutboxDispatcher publica no broker depois, com publisher confirms, e marca
/// DispatchedAt. Garantia resultante: at-least-once — duplicatas são possíveis
/// e o consumidor precisa ser idempotente (e é).
/// </summary>
public class OutboxMessage
{
    public Guid Id { get; set; }
    public required string Type { get; set; }
    public required string RoutingKey { get; set; }
    public required string Payload { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? DispatchedAt { get; set; }

    /// <summary>
    /// W3C traceparent capturado no request original. O dispatcher publica em outro
    /// momento/thread — sem isso, o trace quebraria na fronteira da outbox.
    /// </summary>
    public string? TraceParent { get; set; }
    public int Attempts { get; set; }
    public string? LastError { get; set; }
}
