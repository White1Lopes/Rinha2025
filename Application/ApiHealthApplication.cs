using System.Text.Json;
using Rinha2025.Records;
using Rinha2025.Utils;
using StackExchange.Redis;

namespace Rinha2025.Application;

public interface IApiHealthApplicaiton
{
    Task<ApiHealthStatus> IsHealthyAsync(string processorName);
    Task<ApiHealthStatus> GetHealthStatusAsync(string processorName);
}

public record ApiHealthStatus(bool IsHealthy, DateTime LastCheck, int ConsecutiveFailures, int MinResponseTime);

public class ApiHealthApplication(
    IConnectionMultiplexer multiplexer,
    IHttpClientFactory httpClientFactory) : IApiHealthApplicaiton
{
    private readonly IDatabase _database = multiplexer.GetDatabase();
    private readonly HttpClient _httpClientDefault = httpClientFactory.CreateClient(Constants.DefaultProcessorName);
    private readonly HttpClient _httpClientFallback = httpClientFactory.CreateClient(Constants.FallbackProcessorName);
    private const string _cacheKey = "health";

    public async Task<ApiHealthStatus> IsHealthyAsync(string processorName)
    {
        try
        {
            var cachedData = await _database.HashGetAllAsync($"{_cacheKey}:{processorName}");
            if (cachedData.Length > 0)
            {
                var isHealthy = bool.Parse(cachedData.First(h => h.Name == "IsHealthy").Value!);
                var lastCheck = DateTime.Parse(cachedData.First(h => h.Name == "LastCheck").Value!);
                var failures = int.Parse(cachedData.First(h => h.Name == "ConsecutiveFailures").Value!);
                var minResponseTime = int.Parse(cachedData.FirstOrDefault(h => h.Name == "MinResponseTime").Value!);


                return new ApiHealthStatus(isHealthy, lastCheck, failures, minResponseTime);
            }
        }
        catch (Exception ex)
        {
            return new ApiHealthStatus(false, DateTime.Now, 0, 0);
        }
        return new ApiHealthStatus(false, DateTime.Now, 0, 0);
    }

    public async Task<ApiHealthStatus> GetHealthStatusAsync(string processorName)
    {
        var cacheKey = $"{_cacheKey}:{processorName}";

        // try
        // {
        //     var cachedData = await _database.HashGetAllAsync(cacheKey);
        //     if (cachedData.Length > 0)
        //     {
        //         var isHealthy = bool.Parse(cachedData.First(h => h.Name == "IsHealthy").Value!);
        //         var lastCheck = DateTime.Parse(cachedData.First(h => h.Name == "LastCheck").Value!);
        //         var failures = int.Parse(cachedData.First(h => h.Name == "ConsecutiveFailures").Value!);
        //         var minResponseTime = int.Parse(cachedData.FirstOrDefault(h => h.Name == "MinResponseTime").Value!);
        //
        //         if (DateTime.UtcNow - lastCheck < TimeSpan.FromSeconds(10))
        //         {
        //             return new ApiHealthStatus(isHealthy, lastCheck, failures,minResponseTime);
        //         }
        //     }
        // }
        // catch (Exception ex)
        // {
        // }


        return await CheckAndUpdateHealthAsync(processorName);
    }

    private async Task<ApiHealthStatus> CheckAndUpdateHealthAsync(string processorName)
    {
        var cacheKey = $"health:{processorName}";
        var isHealthy = false;
        var minResponseTime = 0;
        var consecutiveFailures = 0;

        try
        {
            var lockKey = $"{cacheKey}:lock";
            var lockValue = Guid.NewGuid().ToString();
            var lockAcquired =
                await _database.StringSetAsync(lockKey, lockValue, TimeSpan.FromSeconds(10), When.NotExists);

            if (!lockAcquired)
            {
                var cachedData = await _database.HashGetAllAsync(cacheKey);
                if (cachedData.Length > 0)
                {
                    var cachedHealthy = bool.Parse(cachedData.First(h => h.Name == "IsHealthy").Value!);
                    var cachedLastCheck = DateTime.Parse(cachedData.First(h => h.Name == "LastCheck").Value!);
                    var cachedFailures = int.Parse(cachedData.First(h => h.Name == "ConsecutiveFailures").Value!);
                    var cachedMinResponseTime =
                        int.Parse(cachedData.FirstOrDefault(h => h.Name == "MinResponseTime").Value!);
                    return new ApiHealthStatus(cachedHealthy, cachedLastCheck, cachedFailures, cachedMinResponseTime);
                }

                return new ApiHealthStatus(false, DateTime.UtcNow, 1, 0);
            }

            try
            {
                var failuresStr = await _database.HashGetAsync(cacheKey, "ConsecutiveFailures");
                if (failuresStr.HasValue)
                {
                    int.TryParse(failuresStr!, out consecutiveFailures);
                }

                var httpClient = processorName == Constants.DefaultProcessorName
                    ? _httpClientDefault
                    : _httpClientFallback;


                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var response = await httpClient.GetAsync("/payments/service-health", cts.Token);

                var json = JsonSerializer.Deserialize<HealthCheckResponse>(
                    await response.Content.ReadAsStringAsync(cts.Token),
                    AppJsonSerializerContext.Default.HealthCheckResponse);

                Console.WriteLine(
                    $"json: {JsonSerializer.Serialize(json, AppJsonSerializerContext.Default.HealthCheckResponse)}");

                isHealthy = !json?.Failing ?? false;
                minResponseTime = json?.MinResponseTime ?? 0;

                if (isHealthy)
                {
                    consecutiveFailures = 0;
                }
                else
                {
                    consecutiveFailures++;
                }
            }
            catch (Exception ex)
            {
                consecutiveFailures++;
                isHealthy = false;
            }

            var now = DateTime.UtcNow;
            var status = new ApiHealthStatus(isHealthy, now, consecutiveFailures, minResponseTime);

            try
            {
                await _database.HashSetAsync(cacheKey, [
                    new("IsHealthy", isHealthy.ToString()),
                    new("LastCheck", now.ToString("O")),
                    new("ConsecutiveFailures", consecutiveFailures.ToString()),
                    new("MinResponseTime", minResponseTime.ToString())
                ]);

                await _database.KeyExpireAsync(cacheKey, TimeSpan.FromSeconds(60));
            }
            catch (Exception ex)
            {
            }

            finally
            {
                const string script = @"
                    if redis.call('GET', KEYS[1]) == ARGV[1] then
                        return redis.call('DEL', KEYS[1])
                    else
                        return 0
                    end";
                await _database.ScriptEvaluateAsync(script, new RedisKey[] { lockKey }, new RedisValue[] { lockValue });
            }

            return status;
        }
        catch (Exception ex)
        {
            return new ApiHealthStatus(false, DateTime.UtcNow, consecutiveFailures + 1, 0);
        }
    }
}