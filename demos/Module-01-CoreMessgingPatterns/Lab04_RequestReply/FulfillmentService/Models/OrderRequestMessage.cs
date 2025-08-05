using MessagingContracts;

namespace FulfillmentService.Models;

internal record OrderRequestMessage
{
	internal required OrderRequest OrderRequest { get; set; }
	internal required string CorrelationId { get; set; }
	internal required string ReplyTo { get; set; }
}