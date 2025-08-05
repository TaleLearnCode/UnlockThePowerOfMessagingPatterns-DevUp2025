namespace ShippingSubscriber.Models
{
	public class OrderCreated
	{
		public string OrderId { get; set; } = default!;
		public string ProductName { get; set; } = default!;
		public int Quantity { get; set; }
		public decimal Price { get; set; }
		public DateTime CreatedAt { get; set; }
	}
}