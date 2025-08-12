using System.Text.Json.Serialization;

namespace Rinha2025.Records;

public record struct RequestPaymentRecord(Guid CorrelationId, decimal Amount);

public record struct RequestPaymentProcessorRecord(
    Guid CorrelationId,
    DateTimeOffset RequestedAt,
    decimal Amount
);

public record struct RequestPaymentProcessedRecord(
    Guid CorrelationId,
    DateTimeOffset ProcessedAt,
    decimal Amount,
    string Processor
);

public record struct RequestSummaryPaymentRecord(
    DateTimeOffset To,
    DateTimeOffset From
);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(RequestPaymentRecord))]
[JsonSerializable(typeof(RequestPaymentProcessorRecord))]
[JsonSerializable(typeof(RequestPaymentProcessedRecord))]
[JsonSerializable(typeof(RequestSummaryPaymentRecord))]
[JsonSerializable(typeof(SummaryData))]
[JsonSerializable(typeof(SummaryResponseRecord))]
[JsonSerializable(typeof(HealthCheckResponse))]
public partial class AppJsonSerializerContext : JsonSerializerContext
{
}