namespace MessagingContracts;

/// <summary>
/// Represents the response for a fulfillment operation, including order and customer details,
/// fulfillment status, error information, and metadata about the fulfillment.
/// </summary>
public record FulfillmentResponse
{
	/// <summary>
	/// Gets or sets the unique identifier of the order.
	/// </summary>
	public required string OrderId { get; set; }

	/// <summary>
	/// Gets or sets the unique identifier of the customer associated with the order.
	/// </summary>
	public required string CustomerId { get; set; }

	/// <summary>
	/// Gets or sets a value indicating whether the fulfillment operation was successful.
	/// </summary>
	public bool IsSuccessful { get; set; }

	/// <summary>
	/// Gets or sets the error code if the fulfillment operation failed; otherwise, null.
	/// </summary>
	public string? ErrorCode { get; set; }

	/// <summary>
	/// Gets or sets the error message if the fulfillment operation failed; otherwise, null.
	/// </summary>
	public string? ErrorMessage { get; set; }

	/// <summary>
	/// Gets or sets a value indicating whether the order has been fulfilled.
	/// </summary>
	public bool IsFulfilled { get; set; }

	/// <summary>
	/// Gets or sets the date and time when the order was fulfilled.
	/// </summary>
	public DateTime FulfilledAt { get; set; }

	/// <summary>
	/// Gets or sets the identifier of the entity or user who fulfilled the order.
	/// </summary>
	public string? FulfilledBy { get; set; }
}