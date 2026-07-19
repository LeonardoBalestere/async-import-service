using ClosedXML.Excel;

namespace ImportService.Worker;

/// <summary>
/// Implementação ingênua mantida da Fase 1 para comparação: ClosedXML carrega o
/// workbook INTEIRO em memória (medido: ~1,1 GB de pico para 300 mil linhas).
/// Em produção o worker usa <see cref="StreamingExcelTransactionParser"/>.
/// </summary>
public sealed class ExcelTransactionParser : ITransactionParser
{
    public IEnumerable<ParsedTransaction> Parse(Stream stream)
    {
        using var workbook = new XLWorkbook(stream);
        var sheet = workbook.Worksheet(1);

        var rows = new List<ParsedTransaction>();
        foreach (var row in sheet.RowsUsed().Skip(1))
        {
            rows.Add(new ParsedTransaction(
                RowNumber: row.RowNumber(),
                Date: DateOnly.FromDateTime(row.Cell(1).GetDateTime()),
                Account: row.Cell(2).GetString(),
                Description: row.Cell(3).GetString(),
                Amount: row.Cell(4).GetValue<decimal>()));
        }

        return rows;
    }
}
