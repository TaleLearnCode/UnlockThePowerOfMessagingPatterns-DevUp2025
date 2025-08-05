namespace OrderPublisher.Models;


public class OrderCreated
{
	public string OrderId { get; set; } = default!;
	public string ProductName { get; set; } = default!;
	public int Quantity { get; set; }
	public decimal Price { get; set; }
	public DateTime CreatedAt { get; set; }

	// Observability fields
	public string CorrelationId { get; set; } = default!;
	public string Source { get; set; } = "OrderPublisher";
}