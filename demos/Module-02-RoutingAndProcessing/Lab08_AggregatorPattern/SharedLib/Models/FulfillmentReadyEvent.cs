namespace SharedLib.Models;

public class FulfillmentReadyEvent : ITraceableEvent
{
	public required string OrderId { get; set; }
	public required string CustomerId { get; set; }
	public required string CorrelationId { get; set; }
	public required string SourceSystem { get; set; }
	public required string EventType { get; set; }

	public required OrderCreatedEvent OrderDetails { get; set; }
	public required PaymentConfirmedEvent PaymentDetails { get; set; }
	public required InventoryReservedEvent InventoryDetails { get; set; }
}