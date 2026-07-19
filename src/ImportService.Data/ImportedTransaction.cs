namespace ImportService.Data;

/// <summary>
/// Uma linha importada da planilha (lançamento financeiro).
/// Única por (JobId, RowNumber) — reprocessamento não duplica linhas.
/// </summary>
public class ImportedTransaction
{
    public long Id { get; set; }
    public Guid JobId { get; set; }
    public int RowNumber { get; set; }
    public DateOnly Date { get; set; }
    public required string Account { get; set; }
    public required string Description { get; set; }
    public decimal Amount { get; set; }
}
