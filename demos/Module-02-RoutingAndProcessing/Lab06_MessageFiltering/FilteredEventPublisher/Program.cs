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

Random rand = new();

Console.WriteLine("Press any key to start sending messages...");
Console.ReadKey(true);

using CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
	e.Cancel = true; // Prevent the process from terminating immediately
	cts.Cancel();
	AnsiConsole.MarkupLine("[red]Cancellation requested...[/]");
};

while (!cts.Token.IsCancellationRequested)
{
	Console.WriteLine();
	AnsiConsole.MarkupLine("[yellow]Press [bold]Q[/] to stop sending messages.[/]");

	if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Q)
	{
		AnsiConsole.MarkupLine("[yellow]No longer sending messages...[/]");
		cts.Cancel(); // Trigger cancellation manually
		break;
	}

	string priority = GetPriority();
	string correlationId = ActivityTraceId.CreateRandom().ToString();
	var message = new ServiceBusMessage($"This is a `{priority}` event.");
	message.ApplicationProperties["correlationId"] = correlationId;
	message.ApplicationProperties["publishedAt"] = DateTimeOffset.UtcNow;
	message.ApplicationProperties["priority"] = priority;

	await sender.SendMessageAsync(message, cts.Token); // Pass token to operation
	AnsiConsole.MarkupLine($"[cyan]Published:[/] Priority: {priority} | Correlation: {correlationId}");

	try
	{
		await Task.Delay(2000, cts.Token);
	}
	catch (TaskCanceledException)
	{
		break; // Exit loop if delay was cancelled
	}
}

// ----------------------------------------------------------------------------
// Helper methods
// ----------------------------------------------------------------------------

string GetPriority() => rand.Next(1, 101) switch
{
	<= 25 => "High",
	_ => rand.Next(0, 2) == 0 ? "Medium" : "Low"
};