using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ImportService.Messaging;

/// <summary>
/// Fonte única dos instrumentos de telemetria de negócio. O nome "ImportService"
/// é o que Gateway e Worker registram via AddSource/AddMeter — divergir o nome
/// silencia o sinal sem dar erro.
/// </summary>
public static class ImportTelemetry
{
    public const string SourceName = "ImportService";

    public static readonly ActivitySource ActivitySource = new(SourceName);
    public static readonly Meter Meter = new(SourceName);

    public static readonly Counter<long> RowsImported = Meter.CreateCounter<long>(
        "import.rows", unit: "{rows}", description: "Linhas importadas com sucesso");

    public static readonly Histogram<double> ImportDuration = Meter.CreateHistogram<double>(
        "import.duration", unit: "s", description: "Duração do processamento de um arquivo");
}
