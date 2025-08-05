using Azure;
using Azure.Data.Tables;

namespace Persistence.Models
{
	public class OrderRequestTracking : ITableEntity
	{
		public required string PartitionKey { get; set; }
		public required string RowKey { get; set; }
		public string? CorrelationId { get; set; }
		public string? CustomerId { get; set; }
		public string? OrderId { get; set; }
		public bool IsSuccessful { get; set; }
		public long RequestTicks { get; set; }
		public long ResponseTicks { get; set; }
		public DateTimeOffset? Timestamp { get; set; }
		public ETag ETag { get; set; }
	}
}
