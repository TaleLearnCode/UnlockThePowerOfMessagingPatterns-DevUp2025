namespace SharedLib.Models;

public class PaymentConfirmedEvent : ITraceableEvent
{
	public required string PaymentId { get; set; }
	public required string OrderId { get; set; }
	public required string CustomerId { get; set; }
	public required string CorrelationId { get; set; }
	public required string SourceSystem { get; set; }
	public required string EventType { get; set; }
	public required decimal Amount { get; set; }
	public required DateTime Timestamp { get; set; }
}