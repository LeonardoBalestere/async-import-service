using System.Globalization;
using ExcelDataReader;

namespace ImportService.Worker;

/// <summary>
/// Leitura forward-only via ExcelDataReader: memória constante, independente do
/// tamanho do arquivo. Exige stream com Seek — xlsx é um ZIP e o diretório
/// central fica no FIM do arquivo — por isso o worker baixa para um arquivo
/// temporário antes de parsear, em vez de ler direto do stream de rede.
/// </summary>
public sealed class StreamingExcelTransactionParser : ITransactionParser
{
    // ExcelDataReader referencia o codepage 1252 (herança dos formatos Excel)
    // até para xlsx; o provider existe no runtime mas precisa ser registrado.
    static StreamingExcelTransactionParser()
        => System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

    public IEnumerable<ParsedTransaction> Parse(Stream stream)
    {
        using var reader = ExcelReaderFactory.CreateReader(stream);

        var rowNumber = 0;
        while (reader.Read())
        {
            rowNumber++;
            if (rowNumber == 1)
            {
                continue; // cabeçalho
            }

            yield return new ParsedTransaction(
                RowNumber: rowNumber,
                Date: DateOnly.FromDateTime(reader.GetDateTime(0)),
                Account: reader.GetString(1),
                Description: reader.GetString(2),
                Amount: Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture));
        }
    }
}
