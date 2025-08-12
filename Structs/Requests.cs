namespace Rinha2025.Structs;

public struct PaymentRequestEvent
{
    public Guid CorrelationId { get; set; }
    public decimal Amount { get; set; }
}