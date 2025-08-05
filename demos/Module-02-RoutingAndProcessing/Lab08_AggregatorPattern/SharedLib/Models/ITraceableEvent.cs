namespace SharedLib.Models;

public interface ITraceableEvent
{
	string CorrelationId { get; set; }
	string OrderId { get; set; }
	string CustomerId { get; set; }
	string SourceSystem { get; set; }
	string EventType { get; set; }
}