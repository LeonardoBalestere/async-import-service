using ClosedXML.Excel;

namespace ImportService.Worker;

public sealed record ParsedTransaction(int RowNumber, DateOnly Date, string Account, string Description, decimal Amount);

/// <summary>
/// Lê a planilha no layout: Data | Conta | Descricao | Valor, cabeçalho na linha 1.
/// ClosedXML carrega o workbook INTEIRO em memória — é a implementação ingênua
/// proposital da Fase 1; a Fase 2 troca por leitura em streaming.
/// </summary>
public sealed class ExcelTransactionParser
{
    public IReadOnlyList<ParsedTransaction> Parse(Stream stream)
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
