using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using SharedLib;
using SharedLib.Models;
using Spectre.Console;

AnsiConsole.Write(new FigletText("Orders").Centered().Color(Color.Cyan1));

IConfigurationRoot config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json")
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string topicName = config["ServiceBus:TopicName"]!;
int delayMilliseconds = config.GetValue<int>("Sending:DelayMilliseconds", 2000);

Random rand = new();
string sourceSystem = "OrderProducer";

ServiceBusClient client = new(connectionString);
ServiceBusSender sender = client.CreateSender(topicName);

Console.WriteLine("Press any key to begin sending...");
Console.ReadKey(true);

using CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
	e.Cancel = true;
	cts.Cancel();
	AnsiConsole.MarkupLine("[red]Cancellation requested...[/]");
};

while (!cts.Token.IsCancellationRequested)
{
	OrderCreatedEvent order = new()
	{
		OrderId = Guid.NewGuid().ToString(),
		CustomerId = $"CUST-{rand.Next(1000, 9999)}",
		CorrelationId = Guid.NewGuid().ToString(),
		SourceSystem = sourceSystem,
		EventType = EventTypes.OrderCreated,
		Timestamp = DateTime.UtcNow,
		ProductName = "Widget",
		Quantity = rand.Next(1, 5),
		Price = (decimal)Math.Round(rand.NextDouble() * 100, 2),
		CreatedAt = DateTime.UtcNow
	};

	string json = EventSerializer.Serialize(order);

	ServiceBusMessage msg = new(json)
	{
		ContentType = "application/json",
		MessageId = order.OrderId,
		CorrelationId = order.CorrelationId,
		Subject = "OrderCreated",
		ApplicationProperties =
		{
			["eventType"] = EventTypes.OrderCreated,
			["origin"] = sourceSystem
		}
	};

	await sender.SendMessageAsync(msg, cts.Token);

	AnsiConsole.Write(new Panel($"[green]Order Created[/]\nOrderId: [bold]{order.OrderId}[/]")
		.Header("[green]Message Sent[/]")
		.Border(BoxBorder.Rounded));

	try { await Task.Delay(delayMilliseconds, cts.Token); }
	catch (TaskCanceledException) { break; }
}