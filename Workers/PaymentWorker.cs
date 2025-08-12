using Rinha2025.Records;
using StackExchange.Redis;
using System.Text.Json;
using Rinha2025.Application;
using Rinha2025.Utils;

namespace Rinha2025.Workers;

public class PaymentWorker(
    IConnectionMultiplexer multiplexer,
    IServiceScopeFactory serviceScopeFactory)
    : BackgroundService
{
    private const string QueueKey = Constants.RedisQueueKey;
    private readonly IDatabase _database = multiplexer.GetDatabase();

    private const int BatchSize = 200;
    private const int MaxConcurrency = 10;

    private readonly SemaphoreSlim _semaphore = new(MaxConcurrency, MaxConcurrency);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var results = await _database.ListRightPopAsync(QueueKey, BatchSize);

                if (results.Length == 0)
                {
                    continue;
                }

                var tasks = results
                    .Where(result => result.HasValue)
                    .Select(result => ProcessPaymentWithSemaphoreAsync(result!, stoppingToken));

                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    private async Task ProcessPaymentWithSemaphoreAsync(RedisValue result, CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            var paymentRecord = JsonSerializer.Deserialize<RequestPaymentRecord>(result!,
                AppJsonSerializerContext.Default.RequestPaymentRecord);


            await ProcessPaymentInternalAsync(paymentRecord, cancellationToken);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task ProcessPaymentInternalAsync(RequestPaymentRecord record, CancellationToken cancellationToken)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var paymentApplication = scope.ServiceProvider.GetRequiredService<IPaymentApplication>();

        await paymentApplication.ProcessPaymentAsync(record);
    }
}