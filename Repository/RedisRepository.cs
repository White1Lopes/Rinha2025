using System.Collections.Concurrent;
using System.Text.Json;
using Rinha2025.Records;
using Rinha2025.Utils;
using StackExchange.Redis;

namespace Rinha2025.Repository;

public interface IRedisRepository
{
    void EnqueueAsync(string jsonPayload);
    Task<List<string>> DequeueBatchAsync(int batchSize, DateTimeOffset processedAt);
    Task MarkAsProcessedAsync(string jsonPayload, string jsonPayloadProcessed);
    Task EnqueueProcessingErrorAsync(string jsonPayloadProcessing, string jsonPayload);
    Task<(RedisValue[] processed, RedisValue[] processing)> GetPaymentsOnProcessedAndProcessingAsync();
    Task CleanDatabaseAsync();
}

public class RedisRepository(IConnectionMultiplexer connectionMultiplexer) : IRedisRepository
{
    private readonly IDatabaseAsync _database = connectionMultiplexer.GetDatabase();
    private readonly ConcurrentQueue<string> _queue = new();

    public void EnqueueAsync(string jsonPayload)
    {
        _queue.Enqueue(jsonPayload);
        // await _database.ListLeftPushAsync((RedisKey)Constants.RedisQueueKey, (RedisValue)jsonPayload, When.Always,
        //     CommandFlags.FireAndForget).ConfigureAwait(false);
    }

    public async Task<List<string>> DequeueBatchAsync(int batchSize, DateTimeOffset processedAt)
    {
        // var values = await _database.ListRightPopAsync(Constants.RedisQueueKey, batchSize).ConfigureAwait(false);
        if (_queue.IsEmpty)
        {
            return [];
        }

        var values = new List<string>(batchSize);
        var processingValues = new RedisValue[batchSize];
        int count = 0;

        for (; count < batchSize && _queue.TryDequeue(out var result); count++)
        {
            values.Add(result);
            
            var paymentRecord = JsonSerializer.Deserialize<RequestPaymentRecord>(result,
                AppJsonSerializerContext.Default.RequestPaymentRecord);

            var processorRecord = new RequestPaymentProcessorRecord(
                paymentRecord.CorrelationId, processedAt, paymentRecord.Amount);

            processingValues[count] = JsonSerializer.Serialize(processorRecord,
                AppJsonSerializerContext.Default.RequestPaymentProcessorRecord);
        }
        
        if (count < batchSize)
        {
            Array.Resize(ref processingValues, count);
        }

        await _database.ListLeftPushAsync((RedisKey)Constants.RedisQueueProcessingKey, processingValues, When.Always,
            CommandFlags.FireAndForget);


        return values;
    }

    public async Task MarkAsProcessedAsync(string jsonPayloadProcessing, string jsonPayloadProcessed)
    {
        await _database.ListLeftPushAsync(Constants.RedisPaymentProcessedKey, jsonPayloadProcessed
        ).ConfigureAwait(false);
        _database.ListRemoveAsync(Constants.RedisQueueProcessingKey, jsonPayloadProcessing, 1,
            CommandFlags.FireAndForget).ConfigureAwait(false);
    }

    public async Task EnqueueProcessingErrorAsync(string jsonPayloadProcessing, string jsonPayload)
    {
        _database.ListRemoveAsync(Constants.RedisQueueProcessingKey, jsonPayloadProcessing, 1,
            CommandFlags.FireAndForget).ConfigureAwait(false);
        _queue.Enqueue(jsonPayload);
        // _database.ListLeftPushAsync(Constants.RedisQueueKey, jsonPayload, When.Always, CommandFlags.FireAndForget).ConfigureAwait(false);
    }

    public async Task<(RedisValue[] processed, RedisValue[] processing)> GetPaymentsOnProcessedAndProcessingAsync()
    {
        var processed = await _database.ListRangeAsync(Constants.RedisPaymentProcessedKey).ConfigureAwait(false);
        var processing = await _database.ListRangeAsync(Constants.RedisQueueProcessingKey).ConfigureAwait(false);

        return (processed, processing);
    }

    public async Task CleanDatabaseAsync()
    {
        _ = await _database.ExecuteAsync("FLUSHDB").ConfigureAwait(false);
    }
}