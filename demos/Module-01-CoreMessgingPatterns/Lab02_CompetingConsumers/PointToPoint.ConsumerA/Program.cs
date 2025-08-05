using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

// Load configuration
var config = new ConfigurationBuilder()
		.AddJsonFile("appsettings.json")
		.Build();

var connectionString = config["ServiceBus:ConnectionString"];
var queueName = config["ServiceBus:QueueName"];

await using var client = new ServiceBusClient(connectionString);
var processor = client.CreateProcessor(queueName);

processor.ProcessMessageAsync += async args =>
{
	var body = args.Message.Body.ToString();

	AnsiConsole.MarkupLine($"[green]Consumer A received:[/] {body}");

	await args.CompleteMessageAsync(args.Message);
};

processor.ProcessErrorAsync += async args =>
{
	AnsiConsole.MarkupLine($"[red]Consumer A error:[/] {args.Exception.Message}");
};

await processor.StartProcessingAsync();

AnsiConsole.MarkupLine("[bold yellow]Consumer A listening... Press any key to stop.[/]");
Console.ReadKey();

await processor.StopProcessingAsync();