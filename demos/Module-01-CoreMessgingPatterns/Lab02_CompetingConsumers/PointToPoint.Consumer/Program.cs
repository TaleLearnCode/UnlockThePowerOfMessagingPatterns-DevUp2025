/// <summary>
/// Entry point for the Point to Point Consumer application.
/// This application connects to Azure Service Bus, listens for incoming order messages,
/// and displays them live in the console using Spectre.Console.
/// </summary>
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Collections.Concurrent;
using System.Text.Json;

try
{

	// Display application banner
	AnsiConsole.Write(new FigletText("Point to Point Consumer").Centered().Color(Color.Orange1));
	Console.WriteLine();

	// Start live order display
	ConcurrentQueue<OrderDetails> orders = StartLiveOrderDisplay();

	// Load configuration from appsettings.json
	IConfigurationRoot config = new ConfigurationBuilder()
		.AddJsonFile("appsettings.json")
		.Build();
	string connectionString = config["ServiceBus:ConnectionString"]!;
	string queueName = config["ServiceBus:QueueName"]!;

	// Create Service Bus client and processor
	ServiceBusClient client = new(connectionString);
	ServiceBusProcessor processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions
	{
		AutoCompleteMessages = false,
		MaxConcurrentCalls = 1
	});

	// Register event handlers for message processing
	processor.ProcessMessageAsync += async args =>
	{
		string body = args.Message.Body.ToString();
		try
		{
			OrderDetails? order = JsonSerializer.Deserialize<OrderDetails>(body);
			if (order is not null)
			{
				orders.Enqueue(order);
			}
		}
		catch (JsonException ex)
		{
			AnsiConsole.MarkupLine($"[red]Failed to deserialize message: {ex.Message}[/]");
		}
		await args.CompleteMessageAsync(args.Message);
	};

	// Register event handler for processing errors
	processor.ProcessErrorAsync += args =>
	{
		Console.WriteLine($"[!] Error: {args.Exception.Message}");
		return Task.CompletedTask;
	};

	AnsiConsole.MarkupLine("[yellow]Starting to process messages...[/]");
	AnsiConsole.MarkupLine("[yellow]Press any key to stop processing...[/]");
	await processor.StartProcessingAsync();

	Console.ReadLine();
	await processor.StopProcessingAsync();
	await processor.DisposeAsync();
	await client.DisposeAsync();
}
catch (Exception ex)
{
	AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything | ExceptionFormats.ShowLinks);
}

/// <summary>
/// Starts a live display of incoming orders using Spectre.Console.
/// This method creates a concurrent queue to hold incoming <see cref="OrderDetails"/> objects,
/// sets up a Spectre.Console table for visualizing orders, and launches a background task
/// that continuously updates the table with new orders as they arrive.
/// Returns the queue to which new orders should be enqueued for display.
/// </summary>
/// <returns>
/// A <see cref="ConcurrentQueue{OrderDetails}"/> that holds incoming orders to be displayed live.
/// </returns>
static ConcurrentQueue<OrderDetails> StartLiveOrderDisplay()
{	ConcurrentQueue<OrderDetails> orders = new();	Table table = new Table()		.Border(TableBorder.Rounded)		.Title("")		.AddColumn("Order ID")		.AddColumn("Item")		.AddColumn("Quantity");	LiveDisplay live = AnsiConsole.Live(table)		.Overflow(VerticalOverflow.Crop)		.Cropping(VerticalOverflowCropping.Top);	_ = Task.Run(async () =>	{		await AnsiConsole.Live(table)		.Overflow(VerticalOverflow.Crop)		.Cropping(VerticalOverflowCropping.Top)		.StartAsync(async ctx =>		{			while (true)			{				while (orders.TryDequeue(out OrderDetails? order))				{					table.AddRow(						$"[green]{order.OrderId}[/]",						$"[blue]{order.Item}[/]",						$"[red]{order.Quantity}[/]"						);				}				ctx.Refresh();				await Task.Delay(500);			}		});	});
	return orders;
}

/// <summary>
/// Represents the details of an order.
/// </summary>
/// <param name="OrderId">The unique identifier for the order.</param>
/// <param name="Item">The name of the item ordered.</param>
/// <param name="Quantity">The quantity of the item ordered.</param>
internal record OrderDetails(Guid OrderId, string Item, int Quantity);