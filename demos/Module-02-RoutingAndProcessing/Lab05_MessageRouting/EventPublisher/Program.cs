using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Diagnostics;

AnsiConsole.Write(new FigletText("Publisher").LeftJustified().Color(Color.Yellow));

IConfigurationRoot config = new ConfigurationBuilder()
	.AddJsonFile("appsettings.json")
	.Build();
string connectionString = config["ServiceBus:ConnectionString"]!;
string topicName = config["ServiceBus:TopicName"]!;

ServiceBusClient client = new(connectionString);
ServiceBusSender sender = client.CreateSender(topicName);

var events = new[]
{
		new { Tenant = "Alpha", EventType = "Inventory.Created" },
		new { Tenant = "Beta", EventType = "Order.Placed" },
		new { Tenant = "Alpha", EventType = "Order.Cancelled" }
};

string correlationId = ActivityTraceId.CreateRandom().ToString();

Console.WriteLine("Press any key to publish events...");
Console.ReadKey(true);

foreach (var evt in events)
{
	var message = new ServiceBusMessage($"Event from {evt.Tenant}: {evt.EventType}");
	message.ApplicationProperties["tenant"] = evt.Tenant;
	message.ApplicationProperties["eventType"] = evt.EventType;
	message.ApplicationProperties["correlationId"] = correlationId;
	message.ApplicationProperties["publishedAt"] = DateTimeOffset.UtcNow;

	await sender.SendMessageAsync(message);
	AnsiConsole.MarkupLine($"[cyan]Published:[/] {evt.EventType} → Tenant: {evt.Tenant} | Correlation: {correlationId}");
}

await sender.DisposeAsync();
await client.DisposeAsync();