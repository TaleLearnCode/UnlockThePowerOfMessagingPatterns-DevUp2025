namespace SharedLib.Models;

public class OrderCreatedEvent : ITraceableEvent
{
	public required string OrderId { get; set; }
	public required string CustomerId { get; set; }
	public required string CorrelationId { get; set; }
	public required string SourceSystem { get; set; }
	public required string EventType { get; set; }
	public required DateTime Timestamp { get; set; }
	public required string ProductName { get; set; }
	public required int Quantity { get; set; }
	public required decimal Price { get; set; }
	public required DateTime CreatedAt { get; set; }
}