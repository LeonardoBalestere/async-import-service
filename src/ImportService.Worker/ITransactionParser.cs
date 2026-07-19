namespace ImportService.Worker;

public sealed record ParsedTransaction(int RowNumber, DateOnly Date, string Account, string Description, decimal Amount);

/// <summary>
/// O retorno é IEnumerable para permitir leitura lazy: o worker insere em lotes
/// ENQUANTO lê, sem exigir que o parser materialize o arquivo inteiro.
/// </summary>
public interface ITransactionParser
{
    IEnumerable<ParsedTransaction> Parse(Stream stream);
}
