using Rinha2025.Application;
using Rinha2025.Utils;
using StackExchange.Redis;

namespace Rinha2025.Workers;

public class ApiHealthMonitorWorker(IApiHealthApplicaiton healthService) : BackgroundService
{
    private const int CheckIntervalSeconds = 5;
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                    var tasks = new[]
                    {
                        healthService.GetHealthStatusAsync(Constants.DefaultProcessorName),
                        healthService.GetHealthStatusAsync(Constants.FallbackProcessorName)
                    };

                    await Task.WhenAll(tasks);
                

                await Task.Delay(TimeSpan.FromSeconds(CheckIntervalSeconds), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
               
                await Task.Delay(TimeSpan.FromSeconds(CheckIntervalSeconds), stoppingToken);
            }
        }


    }
    
}
