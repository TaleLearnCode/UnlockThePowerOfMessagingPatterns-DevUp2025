using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using OrderPublisher.Services;
using Spectre.Console;
using Spectre.Console.Json;

IConfigurationRoot config = new ConfigurationBuilder()
	.AddJsonFile("appsettings.json")
	.Build();
string connectionString = config["ServiceBus:ConnectionString"]!;
string topicName = config["ServiceBus:TopicName"]!;

ServiceBusClient client = new(connectionString);
ServiceBusSender sender = client.CreateSender(topicName);

while (true)
{
	OrderPublisher.Models.OrderCreated order = OrderGenerator.Generate();
	ServiceBusMessage message = new(System.Text.Json.JsonSerializer.Serialize(order))
	{
		MessageId = order.OrderId,
		ContentType = "application/json",
	};
	message.ApplicationProperties["CorrelationId"] = order.CorrelationId;
	message.ApplicationProperties["Source"] = order.Source;
	message.ApplicationProperties["EventType"] = "OrderCreated";

	await sender.SendMessageAsync(message);

	AnsiConsole.MarkupLine("[green]Published OrderCreated event[/]");
	AnsiConsole.Write(new JsonText(System.Text.Json.JsonSerializer.Serialize(order)));
	await Task.Delay(2000); // Simulate batch intervals
}