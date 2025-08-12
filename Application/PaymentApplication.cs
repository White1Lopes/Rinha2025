using System.Globalization;
using Rinha2025.Records;
using StackExchange.Redis;
using System.Text.Json;
using Rinha2025.Utils;

namespace Rinha2025.Application;

public interface IPaymentApplication
{
    Task RequestPaymentAsync(RequestPaymentRecord record);
    Task RequestPaymentRawAsync(string jsonPayload);
    Task ProcessPaymentAsync(RequestPaymentRecord paymentRecord);
    Task SaveProcessedPaymentAsync(RequestPaymentProcessedRecord processedRecord);
    Task<SummaryResponseRecord> GetPaymentSummaryAsync(RequestSummaryPaymentRecord request);
    void CleanDatabase();
}

public class PaymentApplication(
    IConnectionMultiplexer multiplexer,
    IHttpClientFactory httpClientFactory,
    IApiHealthApplicaiton healthService) : IPaymentApplication
{
    private const string QueueKey = Constants.RedisQueueKey;
    private readonly IDatabase _database = multiplexer.GetDatabase();
    
    private readonly HttpClient _httpClientDefault = httpClientFactory.CreateClient($"{Constants.DefaultProcessorName}_payments");
    private readonly HttpClient _httpClientFallback = httpClientFactory.CreateClient($"{Constants.FallbackProcessorName}_payments");


    public async Task RequestPaymentAsync(RequestPaymentRecord record)
    {
        await _database.ListLeftPushAsync(QueueKey,
                JsonSerializer.Serialize(record, AppJsonSerializerContext.Default.RequestPaymentRecord))
            .ConfigureAwait(false);
    }

    public Task RequestPaymentRawAsync(string jsonPayload)
    {
        _database.ListLeftPushAsync((RedisKey)QueueKey, (RedisValue)jsonPayload, When.Always, CommandFlags.FireAndForget);
        return Task.CompletedTask;
    }

    public async Task ProcessPaymentAsync(RequestPaymentRecord paymentRecord)
    {
        var jsonPolicy = new JsonSerializerOptions()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            TypeInfoResolver = AppJsonSerializerContext.Default
        };
        var requestedAt = DateTimeOffset.UtcNow;

        var request = new RequestPaymentProcessorRecord(
            paymentRecord.CorrelationId,
            requestedAt,
            paymentRecord.Amount
        );

        var defaultHealthy = await healthService.IsHealthyAsync(Constants.DefaultProcessorName);
        var fallbackHealthy = await healthService.IsHealthyAsync(Constants.FallbackProcessorName);


        if (defaultHealthy)
        {
            try
            {
                var response = await _httpClientDefault.PostAsJsonAsync(
                    "/payments", request, jsonPolicy
                ).ConfigureAwait(false);

                var defaultSuccess = response.IsSuccessStatusCode;

                if (defaultSuccess)
                {
                    _ = Task.Run(() => SaveProcessedPaymentAsync(new RequestPaymentProcessedRecord(
                        paymentRecord.CorrelationId,
                        requestedAt,
                        paymentRecord.Amount,
                        Constants.DefaultProcessorName
                    ))).ConfigureAwait(false);
                    return;
                }
            }
            catch (Exception)
            {
            }
        }

        if (fallbackHealthy)
        {
            try
            {
                var fallbackResponse = await _httpClientFallback.PostAsJsonAsync(
                    "/payments", request, jsonPolicy
                ).ConfigureAwait(false);
                
                
                var fallbackSuccess = fallbackResponse.IsSuccessStatusCode;

                if (fallbackSuccess)
                {
                    _ = Task.Run(()=> SaveProcessedPaymentAsync(new RequestPaymentProcessedRecord(
                        paymentRecord.CorrelationId,
                        requestedAt,
                        paymentRecord.Amount,
                        Constants.FallbackProcessorName
                    ))).ConfigureAwait(false);
                    return;
                }
            }
            catch (Exception e)
            {
            }
        }

        await _database.ListLeftPushAsync(QueueKey,
            JsonSerializer.Serialize(paymentRecord, AppJsonSerializerContext.Default.RequestPaymentRecord));
    }

    public async Task SaveProcessedPaymentAsync(RequestPaymentProcessedRecord processedRecord)
    {
        await SaveProcessedPaymentToListAsync(processedRecord);
    }

    public async Task SaveProcessedPaymentToListAsync(RequestPaymentProcessedRecord processedRecord)
    {
        var paymentData = JsonSerializer.Serialize(processedRecord,
            AppJsonSerializerContext.Default.RequestPaymentProcessedRecord);

        await _database.ListLeftPushAsync(Constants.RedisPaymentProcessedKey, paymentData).ConfigureAwait(false);
    }

    public async Task<SummaryResponseRecord> GetPaymentSummaryAsync(RequestSummaryPaymentRecord request)
    {
        var allPayments = await _database.ListRangeAsync(Constants.RedisPaymentProcessedKey).ConfigureAwait(false);

        if (allPayments.Length == 0)
        {
            return new SummaryResponseRecord(new SummaryData(0, 0), new SummaryData(0, 0));
        }

        var defaultRequest = 0;
        var defaultAmount = 0m;
        var fallbackRequest = 0;
        var fallbackAmount = 0m;

        foreach (var paymentJson in allPayments)
        {
            try
            {
                var payment = JsonSerializer.Deserialize<RequestPaymentProcessedRecord>(
                    paymentJson!, AppJsonSerializerContext.Default.RequestPaymentProcessedRecord);

                if (payment == null) continue;


                if (payment.ProcessedAt < request.From || payment.ProcessedAt > request.To)
                    continue;

                switch (payment.Processor)
                {
                    case Constants.DefaultProcessorName:
                        defaultRequest++;
                        defaultAmount += payment.Amount;
                        break;
                    case Constants.FallbackProcessorName:
                        fallbackRequest++;
                        fallbackAmount += payment.Amount;
                        break;
                }
            }
            catch (Exception)
            {
                continue;
            }
        }

        return new SummaryResponseRecord(
            new SummaryData(defaultRequest, defaultAmount),
            new SummaryData(fallbackRequest, fallbackAmount)
        );
    }

    public void CleanDatabase()
    {
        _database.Execute("FLUSHDB");
        _database.KeyDelete(Constants.RedisQueueKey);
        _database.KeyDelete(Constants.RedisPaymentProcessedKey);
        _database.KeyDelete($"{Constants.RedisPaymentProcessedKey}:list");
    }
}