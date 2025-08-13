using Rinha2025.Records;
using StackExchange.Redis;
using System.Text.Json;
using Rinha2025.Application;
using Rinha2025.Repository;
using Rinha2025.Utils;

namespace Rinha2025.Workers;

public class PaymentWorker
    : BackgroundService
{
    private const string QueueKey = Constants.RedisQueueKey;
    private readonly IServiceScopeFactory serviceScopeFactory;

    private readonly SemaphoreSlim _semaphore;
    private const int BatchSize = 100;
    private readonly int _maxConcurrency;

    public PaymentWorker(
        IServiceScopeFactory serviceScopeFactory)
    {
        this.serviceScopeFactory = serviceScopeFactory;
        ThreadPool.GetMaxThreads( out var workerThreads, out var completionPortThreads);
        _maxConcurrency = Math.Max(1, (int)(completionPortThreads * 2));
        _semaphore = new(_maxConcurrency, _maxConcurrency);
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = serviceScopeFactory.CreateScope();
                var redisRepository = scope.ServiceProvider.GetRequiredService<IRedisRepository>();
                var processedAt = DateTimeOffset.UtcNow;
                var results = await redisRepository.DequeueBatchAsync(BatchSize, processedAt);

                if (results.Count == 0)
                {
                    await Task.Delay(5, stoppingToken);
                    continue;
                }

                var tasks = results
                    .Select(result => ProcessPaymentWithSemaphoreAsync(result!,processedAt, stoppingToken));

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

    private async Task ProcessPaymentWithSemaphoreAsync(RedisValue result,DateTimeOffset processedAt, CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            var paymentRecord = JsonSerializer.Deserialize<RequestPaymentRecord>(result!,
                AppJsonSerializerContext.Default.RequestPaymentRecord);


            using var scope = serviceScopeFactory.CreateScope();
            var paymentApplication = scope.ServiceProvider.GetRequiredService<IPaymentApplication>();

            await paymentApplication.ProcessPaymentAsync(paymentRecord,processedAt);
        }
        finally
        {
            _semaphore.Release();
        }
    }
    
    private async Task ProcessPaymentWithSemaphoreAsync(string result,DateTimeOffset processedAt, CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            var paymentRecord = JsonSerializer.Deserialize<RequestPaymentRecord>(result!,
                AppJsonSerializerContext.Default.RequestPaymentRecord);


            using var scope = serviceScopeFactory.CreateScope();
            var paymentApplication = scope.ServiceProvider.GetRequiredService<IPaymentApplication>();

            await paymentApplication.ProcessPaymentAsync(paymentRecord,processedAt);
        }
        finally
        {
            _semaphore.Release();
        }
    }
}