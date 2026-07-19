using System.Globalization;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace ImportService.Data;

public sealed record JobStatusEntry(
    Guid JobId,
    string Status,
    DateTimeOffset UpdatedAt,
    long ExpiresAtUnixSeconds,
    int? TotalRows,
    string? Error,
    long? Attempt)
{
    /// <summary>
    /// No DynamoDB real, o TTL remove o item em ATÉ ~48h depois de expirar —
    /// é mecanismo de limpeza, não contrato de expiração. Quem garante a
    /// semântica de "expirado" é esta checagem na leitura.
    /// </summary>
    public bool IsExpired(DateTimeOffset now) => now.ToUnixTimeSeconds() >= ExpiresAtUnixSeconds;
}

/// <summary>
/// Status de job no DynamoDB, modelado como item collection:
///   pk = jobId | sk = "LATEST"              → sobrescrito a cada transição (GetItem)
///   pk = jobId | sk = "EVENT#&lt;timestamp&gt;"  → histórico de transições (Query)
/// O ledger durável continua no Postgres; aqui vive o estado efêmero que a API
/// consulta — e que o TTL apaga sozinho.
/// </summary>
public class JobStatusStore(IAmazonDynamoDB dynamo, string tableName, int ttlSeconds)
{
    public async Task EnsureTableAsync(CancellationToken ct = default)
    {
        try
        {
            await dynamo.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                BillingMode = BillingMode.PAY_PER_REQUEST,
                AttributeDefinitions =
                [
                    new AttributeDefinition("pk", ScalarAttributeType.S),
                    new AttributeDefinition("sk", ScalarAttributeType.S),
                ],
                KeySchema =
                [
                    new KeySchemaElement("pk", KeyType.HASH),
                    new KeySchemaElement("sk", KeyType.RANGE),
                ],
            }, ct);
        }
        catch (ResourceInUseException)
        {
            // tabela já existe
        }

        try
        {
            await dynamo.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = tableName,
                TimeToLiveSpecification = new TimeToLiveSpecification
                {
                    AttributeName = "expiresAt",
                    Enabled = true,
                },
            }, ct);
        }
        catch (AmazonDynamoDBException)
        {
            // TTL já habilitado — o serviço rejeita reconfiguração repetida
        }
    }

    public async Task WriteTransitionAsync(Guid jobId, string status, int? totalRows = null,
        string? error = null, long? attempt = null, CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow;
        var expiresAt = now.AddSeconds(ttlSeconds).ToUnixTimeSeconds();

        await dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = BuildItem(jobId, "LATEST", status, now, expiresAt, totalRows, error, attempt),
        }, ct);

        await dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = BuildItem(jobId, $"EVENT#{now:O}", status, now, expiresAt, totalRows, error, attempt),
        }, ct);
    }

    public async Task<JobStatusEntry?> GetLatestAsync(Guid jobId, CancellationToken ct = default)
    {
        var response = await dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new(jobId.ToString()),
                ["sk"] = new("LATEST"),
            },
        }, ct);

        if (response.Item is null || response.Item.Count == 0)
        {
            return null;
        }

        var entry = ToEntry(response.Item);
        return entry.IsExpired(DateTimeOffset.UtcNow) ? null : entry;
    }

    public async Task<IReadOnlyList<JobStatusEntry>> GetTimelineAsync(Guid jobId, CancellationToken ct = default)
    {
        var response = await dynamo.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new(jobId.ToString()),
                [":prefix"] = new("EVENT#"),
            },
            // O SK é o timestamp: a ordenação cronológica vem da própria chave.
            ScanIndexForward = true,
        }, ct);

        var now = DateTimeOffset.UtcNow;
        return (response.Items ?? []).Select(ToEntry).Where(e => !e.IsExpired(now)).ToList();
    }

    private static Dictionary<string, AttributeValue> BuildItem(Guid jobId, string sk, string status,
        DateTimeOffset now, long expiresAt, int? totalRows, string? error, long? attempt)
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["pk"] = new(jobId.ToString()),
            ["sk"] = new(sk),
            ["status"] = new(status),
            ["updatedAt"] = new(now.ToString("O")),
            // TTL exige atributo numérico com epoch em SEGUNDOS.
            ["expiresAt"] = new() { N = expiresAt.ToString(CultureInfo.InvariantCulture) },
        };

        if (totalRows.HasValue)
        {
            item["totalRows"] = new() { N = totalRows.Value.ToString(CultureInfo.InvariantCulture) };
        }

        if (error is not null)
        {
            item["error"] = new(error);
        }

        if (attempt.HasValue)
        {
            item["attempt"] = new() { N = attempt.Value.ToString(CultureInfo.InvariantCulture) };
        }

        return item;
    }

    private static JobStatusEntry ToEntry(Dictionary<string, AttributeValue> item) => new(
        Guid.Parse(item["pk"].S),
        item["status"].S,
        DateTimeOffset.Parse(item["updatedAt"].S, CultureInfo.InvariantCulture),
        long.Parse(item["expiresAt"].N, CultureInfo.InvariantCulture),
        item.TryGetValue("totalRows", out var totalRows) ? int.Parse(totalRows.N, CultureInfo.InvariantCulture) : null,
        item.TryGetValue("error", out var error) ? error.S : null,
        item.TryGetValue("attempt", out var attempt) ? long.Parse(attempt.N, CultureInfo.InvariantCulture) : null);
}
