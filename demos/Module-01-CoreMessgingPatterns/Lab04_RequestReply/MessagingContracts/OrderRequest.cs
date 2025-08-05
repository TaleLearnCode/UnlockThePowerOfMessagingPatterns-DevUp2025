namespace MessagingContracts;

/// <summary>
/// Represents a rquest for data from the Fulfillment Service.
/// </summary>
public record OrderRequest
{
	/// <summary>
	/// Gets or sets the unique identifier for the order.
	/// </summary>
	public required string OrderId { get; set; }

	/// <summary>
	/// Gets or sets the unique identifier for the customer placing the order.
	/// </summary>
	public required string CustomerId { get; set; }
}