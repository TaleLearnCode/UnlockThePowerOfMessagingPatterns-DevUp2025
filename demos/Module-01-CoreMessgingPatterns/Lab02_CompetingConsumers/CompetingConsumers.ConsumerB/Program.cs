// This program implements Consumer B for an Azure Service Bus queue.
// It loads configuration from appsettings.json, connects to the Service Bus,
// and listens for messages on the specified queue. When a message is received,
// it prints the message body to the console using Spectre.Console for formatting,
// and then completes the message. Errors during processing are also logged to the console.
// The consumer runs until a key is pressed, at which point it stops processing and exits.

using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

// Load configuration
IConfigurationRoot config = new ConfigurationBuilder()
	.AddJsonFile("appsettings.json")
	.Build();
string connectionString = config["ServiceBus:ConnectionString"]!;
string queueName = config["ServiceBus:QueueName"]!;

// Create the Sevice Bus client and processor
await using ServiceBusClient client = new(connectionString);
ServiceBusProcessor processor = client.CreateProcessor(queueName);

// Register event handlers for message processing
processor.ProcessMessageAsync += async args =>
{
	string body = args.Message.Body.ToString();

	AnsiConsole.MarkupLine($"[green]Consumer B received:[/] {body}");

	await args.CompleteMessageAsync(args.Message);
};

// Register event handler for processing errors
processor.ProcessErrorAsync += args =>
{
	AnsiConsole.MarkupLine($"[red]Consumer B error:[/] {args.Exception.Message}");
	return Task.CompletedTask;
};

// Start processing incoming messages
await processor.StartProcessingAsync();

AnsiConsole.MarkupLine("[bold yellow]Consumer B listening... Press any key to stop.[/]");
Console.ReadKey();

await processor.StopProcessingAsync();