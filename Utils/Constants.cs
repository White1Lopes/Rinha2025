namespace Rinha2025.Utils;

public static class Constants
{
    public const string RedisQueueKey = "payment:queue";
    public const string RedisPaymentProcessedKey = "payment:processed";
    public const string DefaultProcessorName = "default";
    public const string DefaultProcessorUrl = "http://payment-processor-default:8080";
    public const string FallbackProcessorName = "fallback";
    public const string FallbackProcessorUrl = "http://payment-processor-fallback:8080";
}