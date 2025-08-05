using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using SharedLib;
using SharedLib.Models;
using Spectre.Console;

AnsiConsole.Write(new FigletText("Aggregator").Centered().Color(Color.SpringGreen3));

IConfiguration config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json")
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string orderTopic = config["ServiceBus:OrderTopic"]!;
string inventoryTopic = config["ServiceBus:InventoryTopic"]!;
string paymentTopic = config["ServiceBus:PaymentTopic"]!;
string fulfillmentReadyTopic = config["ServiceBus:FullfillmentReadyTopic"]!;
string orderSubscription = config["ServiceBus:OrderSubscription"]!;
string inventorySubscription = config["ServiceBus:InventorySubscription"]!;
string paymentSubscription = config["ServiceBus:PaymentSubscription"]!;
int maxWait = config.GetValue("Processing:MaxWaitSeconds", 10);
string sourceSystem = "Aggregator";

ServiceBusClient client = new(connectionString);

ServiceBusSender fulfillmentSender = client.CreateSender(fulfillmentReadyTopic);

Dictionary<string, OrderCreatedEvent> orders = [];
Dictionary<string, InventoryReservedEvent> reservations = [];
Dictionary<string, PaymentConfirmedEvent> payments = [];

ServiceBusProcessor orderProc = client.CreateProcessor(orderTopic, orderSubscription);
orderProc.ProcessMessageAsync += async args =>
{
	await ProcessMessageAsync(args.Message, EventTypes.OrderCreated);
	await args.CompleteMessageAsync(args.Message);
};
orderProc.ProcessErrorAsync += e =>
{
	AnsiConsole.MarkupLine($"[red]Order Error:[/] {e.Exception.Message}");
	return Task.CompletedTask;
};

ServiceBusProcessor invProc = client.CreateProcessor(inventoryTopic, inventorySubscription);
invProc.ProcessMessageAsync += async args =>
{
	await ProcessMessageAsync(args.Message, EventTypes.InventoryReserved);
	await args.CompleteMessageAsync(args.Message);
};
invProc.ProcessErrorAsync += e =>
{
	AnsiConsole.MarkupLine($"[red]Inventory Error:[/] {e.Exception.Message}");
	return Task.CompletedTask;
};

ServiceBusProcessor paymentProc = client.CreateProcessor(paymentTopic, paymentSubscription);
paymentProc.ProcessMessageAsync += async args =>
{
	await ProcessMessageAsync(args.Message, EventTypes.PaymentConfirmed);
	await args.CompleteMessageAsync(args.Message);
};
paymentProc.ProcessErrorAsync += e =>
{
	AnsiConsole.MarkupLine($"[red]Payment Error:[/] {e.Exception.Message}");
	return Task.CompletedTask;
};

await orderProc.StartProcessingAsync();
await invProc.StartProcessingAsync();
await paymentProc.StartProcessingAsync();

AnsiConsole.MarkupLine("[gray]Aggregator running...[/]");
Console.ReadKey(true);
AnsiConsole.MarkupLine("[red]Stopping Aggregator.[/]");
await orderProc.StopProcessingAsync();
await invProc.StopProcessingAsync();

async Task TryAggregate(string correlationId)
{
	if (orders.TryGetValue(correlationId, out var order) &&
			reservations.TryGetValue(correlationId, out var reservation) &&
			payments.TryGetValue(correlationId, out var payment))
	{

		FulfillmentReadyEvent fulfillment = new()
		{
			OrderId = order.OrderId,
			CustomerId = order.CustomerId,
			CorrelationId = correlationId,
			SourceSystem = "Aggregator",
			EventType = EventTypes.FulfillmentReady,
			OrderDetails = order,
			PaymentDetails = payment,
			InventoryDetails = reservation
		};

		string json = EventSerializer.Serialize(fulfillment);

		ServiceBusMessage msg = new(json)
		{
			MessageId = fulfillment.OrderId,
			CorrelationId = fulfillment.CorrelationId,
			ContentType = "application/json",
			Subject = EventTypes.FulfillmentReady,
			ApplicationProperties =
			{
				["eventType"] = EventTypes.FulfillmentReady,
				["origin"] = sourceSystem
			}
		};

		await fulfillmentSender.SendMessageAsync(msg);

		AnsiConsole.MarkupLine($"[bold green]Aggregated Order Ready for Fulfillment:[/] {order.OrderId}");
		AnsiConsole.MarkupLine($"Customer: {order.CustomerId}");
		AnsiConsole.MarkupLine($"Items: {string.Join(", ", reservation.ItemIds)}");
		AnsiConsole.MarkupLine($"Paid Amount: {payment.Amount:C}");
		AnsiConsole.MarkupLine($"Timestamp: {payment.Timestamp:u}");

		orders.Remove(correlationId);
		reservations.Remove(correlationId);
		payments.Remove(correlationId);
	}
}

async Task ProcessMessageAsync(ServiceBusReceivedMessage msg, string type)
{
	string raw = msg.Body.ToString();
	string correlationId = msg.CorrelationId;

	if (type == EventTypes.OrderCreated)
		orders[correlationId] = EventSerializer.Deserialize<OrderCreatedEvent>(raw);

	if (type == EventTypes.InventoryReserved)
		reservations[correlationId] = EventSerializer.Deserialize<InventoryReservedEvent>(raw);

	if (type == EventTypes.PaymentConfirmed)
		payments[correlationId] = EventSerializer.Deserialize<PaymentConfirmedEvent>(raw);

	await TryAggregate(correlationId);
}