using Amazon.Runtime;
using Amazon.S3;
using ImportService.Data;
using ImportService.Worker;
using Microsoft.EntityFrameworkCore;
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

builder.Services.AddSingleton<ExcelTransactionParser>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
