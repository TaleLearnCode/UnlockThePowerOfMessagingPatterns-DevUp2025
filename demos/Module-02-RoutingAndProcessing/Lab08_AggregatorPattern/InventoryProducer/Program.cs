using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using SharedLib;
using SharedLib.Models;
using Spectre.Console;

AnsiConsole.Write(new FigletText("Inventory").Centered().Color(Color.Orange1));

IConfiguration config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json")
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string ordersTopic = config["ServiceBus:OrdersTopic"]!;
string inventoryTopic = config["ServiceBus:InventoryTopic"]!;
string subscription = config["ServiceBus:Subscription"]!;
int delayMs = config.GetValue("Processing:DelayMilliseconds", 1000);
string sourceSystem = "InventoryProducer";

ServiceBusClient client = new(connectionString);
ServiceBusProcessor processor = client.CreateProcessor(ordersTopic, subscription);
ServiceBusSender sender = client.CreateSender(inventoryTopic);

processor.ProcessMessageAsync += async args =>
{
	string raw = args.Message.Body.ToString();
	OrderCreatedEvent order = EventSerializer.Deserialize<OrderCreatedEvent>(raw);

	await Task.Delay(delayMs); // Simulate inventory logic

	InventoryReservedEvent reservation = new()
	{
		ReservationId = $"INV-{Guid.NewGuid()}",
		OrderId = order.OrderId,
		CustomerId = order.CustomerId,
		CorrelationId = order.CorrelationId,
		SourceSystem = sourceSystem,
		EventType = EventTypes.InventoryReserved,
		ItemIds = ["ITEM-1001", "ITEM-1002"],
		Timestamp = DateTime.UtcNow
	};

	string payload = EventSerializer.Serialize(reservation);

	ServiceBusMessage msg = new(payload)
	{
		MessageId = reservation.ReservationId,
		CorrelationId = reservation.CorrelationId,
		ContentType = "application/json",
		Subject = EventTypes.InventoryReserved,
		ApplicationProperties =
		{
			["eventType"] = EventTypes.InventoryReserved,
			["origin"] = sourceSystem
		}
	};

	await sender.SendMessageAsync(msg);
	AnsiConsole.MarkupLine($"[green]Inventory Reserved for Order:[/] [bold]{order.OrderId}[/]");
	await args.CompleteMessageAsync(args.Message);
};

processor.ProcessErrorAsync += e =>
{
	AnsiConsole.MarkupLine($"[red]Error:[/] {e.Exception.Message}");
	return Task.CompletedTask;
};

await processor.StartProcessingAsync();
AnsiConsole.MarkupLine("[gray]Listening for new orders...[/]");
Console.ReadKey(true);
AnsiConsole.MarkupLine("[red]Shutting down.[/]");
await processor.StopProcessingAsync();