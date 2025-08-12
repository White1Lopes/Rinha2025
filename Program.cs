using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text.Json;
using Disruptor.Dsl;
using Microsoft.AspNetCore.Mvc;
using Polly;
using Rinha2025.Application;
using Rinha2025.Records;
using Rinha2025.Structs;
using Rinha2025.Utils;
using Rinha2025.Workers;
using StackExchange.Redis;

var builder = WebApplication.CreateSlimBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MaxRequestBodySize = 512;
    options.Limits.RequestHeadersTimeout = TimeSpan.FromMilliseconds(500);
    options.Limits.KeepAliveTimeout = TimeSpan.FromSeconds(30);
    options.Limits.MaxConcurrentConnections = 10000;
    options.Limits.MaxConcurrentUpgradedConnections = 10000;
    
    options.AllowSynchronousIO = false;
    options.AddServerHeader = false;
});



var initRedisPolicy = Policy
    .Handle<Exception>()
    .WaitAndRetry(
        retryCount: 60,
        sleepDurationProvider: _ => TimeSpan.FromSeconds(1),
        onRetry: (exception, timeSpan, retryCount, context) =>
        {
            Console.WriteLine($"Retry {retryCount}: {exception.GetType().Name} - {exception.Message}");
        });

builder.Services.AddSingleton<IConnectionMultiplexer>(provider =>
{
    var connectionString = builder.Configuration.GetConnectionString("Redis") ?? "redis:6379";
    var configurationOptions = ConfigurationOptions.Parse(connectionString);
    return initRedisPolicy.Execute(() =>
    {
        var multiplexer = ConnectionMultiplexer.Connect(configurationOptions);
        if (!multiplexer.IsConnected)
        {
            throw new Exception("Redis connection failed (IsConnected = false)");
        }

        return multiplexer;
    });
});

builder.Services.AddScoped<IPaymentApplication, PaymentApplication>();
builder.Services.AddSingleton<IApiHealthApplicaiton, ApiHealthApplication>();


if (builder.Configuration.GetValue<string>("APP_NAME") == "api1")
{
    builder.Services.AddHostedService<ApiHealthMonitorWorker>();
}

builder.Services.AddHostedService<PaymentWorker>();


builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
});

builder.Services.AddHttpClient($"{Constants.DefaultProcessorName}_payments",
        opt =>
        {
            opt.BaseAddress = new Uri(builder.Configuration["default_url"] ?? Constants.DefaultProcessorUrl);
            opt.Timeout = TimeSpan.FromSeconds(2); 
            opt.DefaultRequestHeaders.ConnectionClose = false;
            opt.DefaultRequestHeaders.Add("Connection", "keep-alive");
            opt.DefaultRequestVersion = HttpVersion.Version11;
            opt.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        })
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler()
    {
        MaxConnectionsPerServer = 50,
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
        EnableMultipleHttp2Connections = false,
        ConnectTimeout = TimeSpan.FromMilliseconds(300), 
        
        AutomaticDecompression = DecompressionMethods.GZip,
        
        UseCookies = false, 
    })
    .SetHandlerLifetime(TimeSpan.FromMinutes(5));

builder.Services.AddHttpClient($"{Constants.FallbackProcessorName}_payments",
        opt =>
        {
            opt.BaseAddress = new Uri(builder.Configuration["fallback_url"] ?? Constants.FallbackProcessorUrl);
            opt.Timeout = TimeSpan.FromSeconds(2);
            opt.DefaultRequestHeaders.ConnectionClose = false;
            opt.DefaultRequestHeaders.Add("Connection", "keep-alive");
            opt.DefaultRequestVersion = HttpVersion.Version11;
            opt.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        })
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler()
    {
        MaxConnectionsPerServer = 50,
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
        EnableMultipleHttp2Connections = false,
        ConnectTimeout = TimeSpan.FromMilliseconds(300), 
        
        AutomaticDecompression = DecompressionMethods.GZip,
        
        UseCookies = false,
    })
    .SetHandlerLifetime(TimeSpan.FromMinutes(5));

builder.Services.AddHttpClient(Constants.DefaultProcessorName,
        opt =>
        {
            opt.BaseAddress = new Uri(builder.Configuration["default_url"] ?? Constants.DefaultProcessorUrl);
            opt.Timeout = TimeSpan.FromSeconds(10); 
            opt.DefaultRequestHeaders.ConnectionClose = false;
        })
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler()
    {
        MaxConnectionsPerServer = 10,
        PooledConnectionLifetime = TimeSpan.FromMinutes(15),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
        EnableMultipleHttp2Connections = true,
        ConnectTimeout = TimeSpan.FromSeconds(2), 
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    })
    .SetHandlerLifetime(TimeSpan.FromMinutes(15));

builder.Services.AddHttpClient(Constants.FallbackProcessorName,
        opt =>
        {
            opt.BaseAddress = new Uri(builder.Configuration["fallback_url"] ?? Constants.FallbackProcessorUrl);
            opt.Timeout = TimeSpan.FromSeconds(10);
            opt.DefaultRequestHeaders.ConnectionClose = false;
        })
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler()
    {
        MaxConnectionsPerServer = 10,
        PooledConnectionLifetime = TimeSpan.FromMinutes(15),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
        EnableMultipleHttp2Connections = true,
        ConnectTimeout = TimeSpan.FromSeconds(2),
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    })
    .SetHandlerLifetime(TimeSpan.FromMinutes(15));

var app = builder.Build();



app.MapPost("/payments", async (HttpContext context, IPaymentApplication paymentApplication) =>
{
    var reader = context.Request.BodyReader;
    var result = await reader.ReadAsync();
    var buffer = result.Buffer;
    
    var rentedArray = ArrayPool<byte>.Shared.Rent((int)buffer.Length);
    try
    {
        buffer.CopyTo(rentedArray);
        var jsonContent = System.Text.Encoding.UTF8.GetString(rentedArray, 0, (int)buffer.Length);
        reader.AdvanceTo(buffer.End);
        
        _ = paymentApplication.RequestPaymentRawAsync(jsonContent);
        return Results.Accepted();
    }
    finally
    {
        ArrayPool<byte>.Shared.Return(rentedArray);
    }
});


app.MapGet("/payments-summary",
    async ([FromQuery] DateTimeOffset to,
            [FromQuery] DateTimeOffset from, [FromServices] IPaymentApplication paymentApplication) =>
        Results.Ok(await paymentApplication.GetPaymentSummaryAsync(new RequestSummaryPaymentRecord(to, from))));

app.MapGet("clean-database", ([FromServices] IPaymentApplication paymentApplication) =>
{
    paymentApplication.CleanDatabase();
    return Results.Ok("Database cleaned");
});

app.Run();