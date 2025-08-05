namespace SharedLib.Models;

public class InventoryReservedEvent : ITraceableEvent
{
	public required string ReservationId { get; set; }
	public required string OrderId { get; set; }
	public required string CustomerId { get; set; }
	public required string CorrelationId { get; set; }
	public required string SourceSystem { get; set; }
	public required string EventType { get; set; }
	public List<string> ItemIds { get; set; } = [];
	public required DateTime Timestamp { get; set; }
}