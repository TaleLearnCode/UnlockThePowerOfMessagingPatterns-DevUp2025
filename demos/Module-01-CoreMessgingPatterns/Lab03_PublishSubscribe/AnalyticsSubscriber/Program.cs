using AnalyticsSubscriber.Models;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using Spectre.Console.Json;

IConfigurationRoot config = new ConfigurationBuilder()
	.AddJsonFile("appsettings.json")
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string topicName = config["ServiceBus:TopicName"]!;
string subscriptionName = config["ServiceBus:SubscriptionName"]!;

ServiceBusClient client = new(connectionString);
ServiceBusProcessor processor = client.CreateProcessor(topicName, subscriptionName, new ServiceBusProcessorOptions
{
	AutoCompleteMessages = false,
	MaxConcurrentCalls = 1
});

processor.ProcessMessageAsync += async args =>
{
	try
	{
		string json = args.Message.Body.ToString();
		OrderCreated order = System.Text.Json.JsonSerializer.Deserialize<OrderCreated>(json)!;

		string correlationId = (string)args.Message.ApplicationProperties["CorrelationId"]!;
		string source = (string)args.Message.ApplicationProperties["Source"]!;
		string eventType = (string)args.Message.ApplicationProperties["EventType"]!;
		int deliveryCount = args.Message.DeliveryCount;
		DateTime enqueuedTime = args.Message.EnqueuedTime.UtcDateTime;

		AnsiConsole.MarkupLine($"[yellow]Received {eventType}[/] from [bold]{source}[/] with CorrelationId: [underline]{correlationId}[/]");
		AnsiConsole.MarkupLine($"[gray]DeliveryAttempt: {deliveryCount}, EnqueuedAt: {enqueuedTime:O}[/]");
		AnsiConsole.Write(new JsonText(args.Message.Body.ToString()));

		await args.CompleteMessageAsync(args.Message);
	}
	catch (Exception ex)
	{
		AnsiConsole.MarkupLine($"[red]Error processing message: {ex.Message}[/]");
		await args.AbandonMessageAsync(args.Message);
	}
};

processor.ProcessErrorAsync += args =>
{
	AnsiConsole.MarkupLine($"[red]Receiver error: {args.Exception.Message}[/]");
	return Task.CompletedTask;
};

AnsiConsole.MarkupLine($"[green]Listening on subscription '{subscriptionName}'...[/]");
await processor.StartProcessingAsync();

await Task.Delay(-1); // Keep the app running