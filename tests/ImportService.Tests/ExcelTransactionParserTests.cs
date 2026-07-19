using ClosedXML.Excel;
using ImportService.Worker;

namespace ImportService.Tests;

public class ExcelTransactionParserTests
{
    private static MemoryStream BuildWorkbook(params (DateTime Date, string Account, string Description, decimal Amount)[] rows)
    {
        var stream = new MemoryStream();
        using (var workbook = new XLWorkbook())
        {
            var sheet = workbook.AddWorksheet("Lancamentos");
            sheet.Cell(1, 1).Value = "Data";
            sheet.Cell(1, 2).Value = "Conta";
            sheet.Cell(1, 3).Value = "Descricao";
            sheet.Cell(1, 4).Value = "Valor";

            for (var i = 0; i < rows.Length; i++)
            {
                sheet.Cell(i + 2, 1).Value = rows[i].Date;
                sheet.Cell(i + 2, 2).Value = rows[i].Account;
                sheet.Cell(i + 2, 3).Value = rows[i].Description;
                sheet.Cell(i + 2, 4).Value = rows[i].Amount;
            }

            workbook.SaveAs(stream);
        }

        stream.Position = 0;
        return stream;
    }

    [Fact]
    public void Parse_le_todas_as_linhas_de_dados_pulando_o_cabecalho()
    {
        using var stream = BuildWorkbook(
            (new DateTime(2026, 1, 10), "ACC-0001", "Pagamento fornecedor", -1500.50m),
            (new DateTime(2026, 1, 11), "ACC-0002", "Recebimento cliente", 3200.00m));

        var rows = new ExcelTransactionParser().Parse(stream);

        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public void Parse_preserva_os_valores_tipados_de_cada_coluna()
    {
        using var stream = BuildWorkbook((new DateTime(2026, 3, 5), "ACC-0042", "Tarifa bancária", -12.34m));

        var row = Assert.Single(new ExcelTransactionParser().Parse(stream));

        Assert.Equal(new DateOnly(2026, 3, 5), row.Date);
        Assert.Equal("ACC-0042", row.Account);
        Assert.Equal("Tarifa bancária", row.Description);
        Assert.Equal(-12.34m, row.Amount);
        Assert.Equal(2, row.RowNumber);
    }

    [Fact]
    public void Parse_planilha_so_com_cabecalho_retorna_vazio()
    {
        using var stream = BuildWorkbook();

        Assert.Empty(new ExcelTransactionParser().Parse(stream));
    }
}
