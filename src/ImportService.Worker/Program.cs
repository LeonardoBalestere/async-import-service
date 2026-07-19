using Amazon.Runtime;
using Amazon.S3;
using ImportService.Data;
using ImportService.Messaging;
using ImportService.Worker;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<ImportDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddSingleton<IAmazonS3>(_ => new AmazonS3Client(
    new BasicAWSCredentials(builder.Configuration["S3:AccessKey"], builder.Configuration["S3:SecretKey"]),
    new AmazonS3Config
    {
        ServiceURL = builder.Configuration["S3:ServiceUrl"],
        ForcePathStyle = true,
    }));

builder.Services.AddSingleton<IConnection>(_ =>
    new ConnectionFactory { HostName = builder.Configuration["RabbitMq:Host"]! }
        .CreateConnectionAsync().GetAwaiter().GetResult());

builder.Services.AddSingleton<ITransactionParser, StreamingExcelTransactionParser>();
builder.Services.AddHostedService<Worker>();
builder.Services.AddHostedService<QueueDepthCollector>();

var otlpEndpoint = new Uri(builder.Configuration["Otlp:Endpoint"] ?? "http://localhost:4317");

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("import-worker"))
    .WithTracing(tracing => tracing
        .AddHttpClientInstrumentation()           // cobre as chamadas S3 (o SDK usa HttpClient)
        .AddSource("Npgsql")                      // spans de SQL nativos do driver
        .AddSource("RabbitMQ.Client.Subscriber")  // deliver nativo do client 7 (extrai traceparent)
        .AddSource(ImportTelemetry.SourceName)
        .AddOtlpExporter(o => o.Endpoint = otlpEndpoint))
    .WithMetrics(metrics => metrics
        .AddHttpClientInstrumentation()
        .AddRuntimeInstrumentation()              // GC, heap — a métrica que conta a história da Fase 2
        .AddMeter("Npgsql")
        .AddMeter(ImportTelemetry.SourceName)
        .AddOtlpExporter(o => o.Endpoint = otlpEndpoint));

builder.Logging.AddOpenTelemetry(logging =>
{
    logging.IncludeFormattedMessage = true;
    logging.IncludeScopes = true;
    logging.AddOtlpExporter(o => o.Endpoint = otlpEndpoint);
});

var host = builder.Build();
host.Run();
