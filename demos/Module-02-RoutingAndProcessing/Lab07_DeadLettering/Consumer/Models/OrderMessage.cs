public class OrderMessage
{
	public string OrderId { get; set; } = default!;
	public string CustomerId { get; set; } = default!;
	public Item[] Items { get; set; } = default!;
	public decimal TotalAmount { get; set; }
	public DateTime Timestamp { get; set; }
}
