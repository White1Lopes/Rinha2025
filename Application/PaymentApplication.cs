using System.Globalization;
using Rinha2025.Records;
using StackExchange.Redis;
using System.Text.Json;
using Polly;
using Polly.Retry;
using Rinha2025.Repository;
using Rinha2025.Utils;

namespace Rinha2025.Application;

public interface IPaymentApplication
{
    Task RequestPaymentAsync(RequestPaymentRecord record);
    Task RequestPaymentRawAsync(string jsonPayload);
    Task ProcessPaymentAsync(RequestPaymentRecord paymentRecord, DateTimeOffset processedAt);
    Task SaveProcessedPaymentAsync(RequestPaymentProcessedRecord processedRecord);
    Task<SummaryResponseRecord> GetPaymentSummaryAsync(RequestSummaryPaymentRecord request);
    void CleanDatabase();
}

public class PaymentApplication(
    IRedisRepository repository,
    IHttpClientFactory httpClientFactory,
    IApiHealthApplicaiton healthService) : IPaymentApplication
{
    private readonly HttpClient _httpClientDefault =
        httpClientFactory.CreateClient($"{Constants.DefaultProcessorName}_payments");

    private readonly HttpClient _httpClientFallback =
        httpClientFactory.CreateClient($"{Constants.FallbackProcessorName}_payments");

    private readonly JsonSerializerOptions _jsonPolicy = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        TypeInfoResolver = AppJsonSerializerContext.Default
    };

    private readonly AsyncRetryPolicy<HttpResponseMessage> _retryPolicy =
        Policy.HandleResult<HttpResponseMessage>(result => !result.IsSuccessStatusCode)
            .Or<HttpRequestException>()
            .Or<TaskCanceledException>()
            .RetryAsync(2);

    public async Task RequestPaymentAsync(RequestPaymentRecord record)
    {
        _ = Task.Run(() =>
            repository.EnqueueAsync(JsonSerializer.Serialize(record,
                AppJsonSerializerContext.Default.RequestPaymentRecord)));
    }

    public Task RequestPaymentRawAsync(string jsonPayload)
    {
        _ = Task.Run(() =>
            repository.EnqueueAsync(jsonPayload));
        return Task.CompletedTask;
    }

    public async Task ProcessPaymentAsync(RequestPaymentRecord paymentRecord, DateTimeOffset requestedAt)
    {
        var request = new RequestPaymentProcessorRecord(
            paymentRecord.CorrelationId,
            requestedAt,
            paymentRecord.Amount
        );

        var defaultHealthy = await healthService.IsHealthyAsync(Constants.DefaultProcessorName);
        var fallbackHealthy = await healthService.IsHealthyAsync(Constants.FallbackProcessorName);

        if (defaultHealthy.IsHealthy && fallbackHealthy.IsHealthy)
        {
            if (defaultHealthy.MinResponseTime <= fallbackHealthy.MinResponseTime)
            {
                if (await CallProcessor(Constants.DefaultProcessorName, request)) return;
                if (await CallProcessor(Constants.FallbackProcessorName, request)) return;
            }
            else
            {
                if (await CallProcessor(Constants.FallbackProcessorName, request)) return;
                if (await CallProcessor(Constants.DefaultProcessorName, request)) return;
            }
        }
        else if (defaultHealthy.IsHealthy)
        {
            if (await CallProcessor(Constants.DefaultProcessorName, request)) return;
        }
        else if (fallbackHealthy.IsHealthy)
        {
            if (await CallProcessor(Constants.FallbackProcessorName, request)) return;
        }

        _ = Task.Run(() => repository.EnqueueProcessingErrorAsync(JsonSerializer.Serialize(request,
                AppJsonSerializerContext.Default.RequestPaymentProcessorRecord),
            JsonSerializer.Serialize(paymentRecord, AppJsonSerializerContext.Default.RequestPaymentRecord)));
    }

    private async Task<bool> CallProcessor(string processorName, RequestPaymentProcessorRecord paymentRecord)
    {
        try
        {
            var httpClient = processorName == Constants.DefaultProcessorName
                ? _httpClientDefault
                : _httpClientFallback;

            var response = await _retryPolicy.ExecuteAsync(async () => await httpClient.PostAsJsonAsync(
            "/payments", paymentRecord, _jsonPolicy
            ).ConfigureAwait(false));

            // var response = await httpClient.PostAsJsonAsync(
            //     "/payments", paymentRecord, _jsonPolicy
            // ).ConfigureAwait(false);
            
            if (!response.IsSuccessStatusCode)
                return false;

            await SaveProcessedPaymentAsync(new RequestPaymentProcessedRecord(
                paymentRecord.CorrelationId,
                paymentRecord.RequestedAt,
                paymentRecord.Amount,
                processorName
            )).ConfigureAwait(false);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public async Task SaveProcessedPaymentAsync(RequestPaymentProcessedRecord processedRecord)
    {
        _ = Task.Run(() =>
            repository.MarkAsProcessedAsync(
                JsonSerializer.Serialize(new RequestPaymentProcessorRecord(
                        processedRecord.CorrelationId,
                        processedRecord.ProcessedAt,
                        processedRecord.Amount),
                    AppJsonSerializerContext.Default.RequestPaymentProcessorRecord),
                JsonSerializer.Serialize(processedRecord,
                    AppJsonSerializerContext.Default.RequestPaymentProcessedRecord)));
    }

    public async Task<SummaryResponseRecord> GetPaymentSummaryAsync(RequestSummaryPaymentRecord request)
    {
        RedisValue[] processed;
        do
        {
            (processed, var processing) = await repository.GetPaymentsOnProcessedAndProcessingAsync();

            var processingInRange = processing
                .Select(x => JsonSerializer.Deserialize<RequestPaymentProcessorRecord>(x,
                    AppJsonSerializerContext.Default.RequestPaymentProcessorRecord))
                .Any(x => x.RequestedAt >= request.From && x.RequestedAt <= request.To);

            if (!processingInRange)
                break;

            await Task.Delay(5);
        } while (true);


        var processedRecords = processed.Select(x =>
                JsonSerializer.Deserialize<RequestPaymentProcessedRecord>(x,
                    AppJsonSerializerContext.Default.RequestPaymentProcessedRecord))
            .ToArray();

        if (processedRecords.Length == 0)
        {
            return new SummaryResponseRecord(new SummaryData(0, 0), new SummaryData(0, 0));
        }

        var defaultRequest = 0;
        var defaultAmount = 0m;
        var fallbackRequest = 0;
        var fallbackAmount = 0m;

        foreach (var payment in processedRecords)
        {
            try
            {
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
        repository.CleanDatabaseAsync();
    }
}