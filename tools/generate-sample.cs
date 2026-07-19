// Gera planilhas de teste no layout Data | Conta | Descricao | Valor.
// Uso: dotnet run tools/generate-sample.cs <caminho-saida.xlsx> <qtde-linhas>
#:package ClosedXML@*

using ClosedXML.Excel;

var path = args.Length > 0 ? args[0] : Path.Combine("samples", "lancamentos-100.xlsx");
var count = args.Length > 1 ? int.Parse(args[1]) : 100;

Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);

var random = new Random(42);
using var workbook = new XLWorkbook();
var sheet = workbook.AddWorksheet("Lancamentos");

sheet.Cell(1, 1).Value = "Data";
sheet.Cell(1, 2).Value = "Conta";
sheet.Cell(1, 3).Value = "Descricao";
sheet.Cell(1, 4).Value = "Valor";

for (var i = 0; i < count; i++)
{
    sheet.Cell(i + 2, 1).Value = new DateTime(2026, 1, 1).AddDays(random.Next(180));
    sheet.Cell(i + 2, 2).Value = $"ACC-{random.Next(1, 50):D4}";
    sheet.Cell(i + 2, 3).Value = $"Lançamento {i + 1}";
    sheet.Cell(i + 2, 4).Value = Math.Round((decimal)(random.NextDouble() * 10000 - 5000), 2);
}

workbook.SaveAs(path);
Console.WriteLine($"{path}: {count} linhas geradas");
