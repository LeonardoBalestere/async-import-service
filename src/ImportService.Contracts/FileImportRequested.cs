namespace ImportService.Contracts;

/// <summary>
/// Publicada pelo Gateway quando o upload termina de ser gravado no object storage.
/// Claim Check: a mensagem carrega a referência do arquivo, nunca o conteúdo.
/// </summary>
public sealed record FileImportRequested
{
    public required Guid JobId { get; init; }

    public required string Bucket { get; init; }

    public required string ObjectKey { get; init; }

    /// <summary>Nome original enviado pelo usuário — apenas informativo, nunca usado como chave.</summary>
    public required string FileName { get; init; }

    public required string ContentType { get; init; }

    public required long FileSizeBytes { get; init; }

    /// <summary>Hash do conteúdo, base da idempotência de upload.</summary>
    public required string FileSha256 { get; init; }

    public required DateTimeOffset UploadedAt { get; init; }

    /// <summary>Versão do contrato. Consumidores antigos podem coexistir com produtores novos durante um rolling deploy.</summary>
    public int SchemaVersion { get; init; } = 1;
}
