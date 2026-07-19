namespace ImportService.Data;

public enum ImportJobStatus
{
    Received,
    Processing,
    Retrying,
    Completed,
    Failed,
}

/// <summary>
/// Registro de idempotência e rastreio: um job por conteúdo de arquivo (FileSha256 único).
/// Criado pelo Gateway antes do publish; atualizado pelo Worker durante o processamento.
/// </summary>
public class ImportJob
{
    public Guid Id { get; set; }
    public required string FileName { get; set; }
    public required string FileSha256 { get; set; }
    public required string Bucket { get; set; }
    public required string ObjectKey { get; set; }
    public long FileSizeBytes { get; set; }
    public ImportJobStatus Status { get; set; }
    public int? TotalRows { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}
