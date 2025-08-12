namespace Rinha2025.Records;

public record SummaryResponseRecord(SummaryData Default, SummaryData Fallback);

public record SummaryData(long TotalRequests, decimal TotalAmount);

public record HealthCheckResponse(
    bool Failing,
    int MinResponseTime
);