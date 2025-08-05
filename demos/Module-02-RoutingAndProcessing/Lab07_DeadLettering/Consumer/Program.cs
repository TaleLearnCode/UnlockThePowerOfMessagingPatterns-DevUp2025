using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Text.Json;

AnsiConsole.Write(new FigletText("Main Consumer").Centered().Color(Color.Green));

// Load configuration
IConfiguration config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json", optional: false)
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string queueName = config["ServiceBus:QueueName"]!;

ServiceBusClient client = new(connectionString);
ServiceBusProcessor processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions
{
	AutoCompleteMessages = false,
	MaxConcurrentCalls = 5
});

// Message handler
processor.ProcessMessageAsync += async args =>
{
	ServiceBusReceivedMessage message = args.Message;
	string body = message.Body.ToString();

	AnsiConsole.Write(new Rule($"[bold]{message.Subject}[/] — {message.MessageId}").RuleStyle("grey"));

	try
	{
		OrderMessage? payload = JsonSerializer.Deserialize<OrderMessage>(body) ?? throw new JsonException("Deserialized payload is null");

		AnsiConsole.Write(
			new Panel($"[green]Parsed Order[/]\nOrderId: [bold]{payload.OrderId}[/]\nCustomerId: {payload.CustomerId}")
				.Header("[green]Valid Message[/]")
				.Border(BoxBorder.Rounded)
		);

		await args.CompleteMessageAsync(message);
	}
	catch (Exception)
	{
		await args.DeadLetterMessageAsync(
			message,
			"DeserializationFailure",
			"Unable to parse message"
		);

		AnsiConsole.Write(
			new Panel($"[red]Schema Violation[/]\nSent to DLQ with reason: DeserializationFailure")
				.Header("[red]Message Re-Routed[/]")
				.Border(BoxBorder.Rounded)
		);
	}
};

// Error handler
processor.ProcessErrorAsync += args =>
{
	AnsiConsole.MarkupLine($"[red bold]Processor error:[/] {args.Exception.Message}");
	return Task.CompletedTask;
};

// Start processing
await processor.StartProcessingAsync();

AnsiConsole.MarkupLine("[yellow]Processor running. Press Ctrl+C to stop...[/]");

using CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
	e.Cancel = true;
	cts.Cancel();
};

// Wait indefinitely until cancellation is requested
try
{
	await Task.Delay(-1, cts.Token);
}
catch (TaskCanceledException)
{
	// Expected when Ctrl+C is pressed
}
await processor.StopProcessingAsync();
await processor.DisposeAsync();
await client.DisposeAsync();

AnsiConsole.MarkupLine("[yellow]Consumer shut down gracefully.[/]");