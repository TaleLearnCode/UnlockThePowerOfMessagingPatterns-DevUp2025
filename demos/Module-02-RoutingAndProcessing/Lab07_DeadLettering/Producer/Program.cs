using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Text.Json;

AnsiConsole.Write(new FigletText("Publisher").LeftJustified().Color(Color.Yellow));

IConfigurationRoot config = new ConfigurationBuilder()
		.SetBasePath(Directory.GetCurrentDirectory())
		.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
		.Build();
string serviceBusConnectionString = config["ServiceBus:ConnectionString"]!;
string queueName = config["ServiceBus:QueueName"]!;
int delayMilliseconds = config.GetValue<int>("Sending:DelayMilliseconds", 2000);

ServiceBusClient client = new(serviceBusConnectionString);
ServiceBusSender sender = client.CreateSender(queueName);

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

	bool isValidSchema = rand.NextDouble() > 0.4; // ~60% valid; ~40% invalid
	ServiceBusMessage message;

	if (isValidSchema)
	{
		message = BuildValidOrderMessage(rand);
	}
	else
	{
		message = BuildInvalidOrderMessage(rand);
	}

	await sender.SendMessageAsync(message, cts.Token);

	try
	{
		await Task.Delay(delayMilliseconds, cts.Token);
	}
	catch (TaskCanceledException)
	{
		break;
	}
}

static ServiceBusMessage BuildValidOrderMessage(Random rand)
{
	ServiceBusMessage message;
	var payload = new
	{
		OrderId = Guid.NewGuid().ToString(),
		CustomerId = $"CUST-{rand.Next(100, 999)}",
		Items = new[] { new { Sku = "SKU-" + rand.Next(1000, 9999), Quantity = rand.Next(1, 4) } },
		TotalAmount = Math.Round(rand.NextDouble() * 500, 2),
		Timestamp = DateTime.UtcNow
	};

	message = new ServiceBusMessage(JsonSerializer.Serialize(payload))
	{
		ContentType = "application/json",
		MessageId = payload.OrderId,
		CorrelationId = "OrderCorrelationTest",
		Subject = "OrderCreated",
		TimeToLive = TimeSpan.FromMinutes(1),
		ApplicationProperties =
		{
			{ "eventType", "OrderCreated" },
			{ "channel", "WebPortal" },
			{ "schemaHint", "GoodSchema" }
		}
	};

	AnsiConsole.Write(
		new Panel($"[green]VALID[/]\nOrderId: [bold]{payload.OrderId}[/]")
			.Border(BoxBorder.Rounded)
			.Header("[green]Message Sent[/]")
	);
	return message;
}

static ServiceBusMessage BuildInvalidOrderMessage(Random rand)
{
	ServiceBusMessage message;
	var sneakyPayload = new
	{
		OrderId = Guid.NewGuid().ToString(),
		CustomerId = $"CUST-{rand.Next(100, 999)}",
		Items = new[]
		{
			new { Sku = "SKU-1234", Quantity = "Two" }	// 'Quantity' should be an int, not a string
    },
		TotalAmount = Math.Round(rand.NextDouble() * 500, 2),
		Timestamp = DateTime.UtcNow
	};

	message = new ServiceBusMessage(JsonSerializer.Serialize(sneakyPayload))
	{
		ContentType = "application/json",
		MessageId = sneakyPayload.OrderId,
		CorrelationId = "SchemaFailTest",
		Subject = "OrderCreated",
		TimeToLive = TimeSpan.FromMinutes(1),
		ApplicationProperties =
		{
			{ "eventType", "OrderCreated" },
			{ "channel", "WebPortal" },
			{ "schemaHint", "QuantityAsString" }
		}
	};

	AnsiConsole.Write(
		new Panel($"[yellow]SNEAKY BAD[/]\nOrderId: [bold]{sneakyPayload.OrderId}[/]\nQuantity is a string!")
			.Border(BoxBorder.Rounded)
			.Header("[yellow]Message Sent[/]")
	);
	return message;
}