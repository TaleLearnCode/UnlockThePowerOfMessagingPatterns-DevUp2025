using Azure.Data.Tables;
using Azure.Messaging.ServiceBus;
using MessagingContracts;
using Microsoft.Extensions.Configuration;
using OrderClient.Services;
using Persistence.Models;
using Spectre.Console;
using System.Text.Json;

AnsiConsole.Write(new FigletText("Order Client").Centered().Color(Color.Green));
Console.WriteLine();

IConfigurationRoot config = new ConfigurationBuilder()
		.SetBasePath(Directory.GetCurrentDirectory())
		.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
		.Build();
string serviceBusConnectionString = config["ServiceBus:ConnectionString"]!;
string requestQueueName = config["ServiceBus:RequestQueueName"]!;
string replyQueueName = config["ServiceBus:ReplyQueueName"]!;
string storageAccountConnectionString = config["Storage:ConnectionString"]!;
string trackingTableName = config["Storage:TrackingTableName"]!;

ServiceBusClient client = new(serviceBusConnectionString);
ServiceBusSender sender = client.CreateSender(requestQueueName);

TableClient tableClient = new(storageAccountConnectionString, trackingTableName);
await tableClient.CreateIfNotExistsAsync();

Console.WriteLine("Press any key to start sending OrderRequest messages...");
Console.ReadKey(true);

while (true)
{

	Console.WriteLine();
	AnsiConsole.MarkupLine("[yellow]Press [bold]Q[/] to stop sending requests.[/]");

	if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Q)
	{
		AnsiConsole.MarkupLine("[yellow]No longer sending requests...[/]");
		break;
	}

	OrderRequest orderRequest = OrderRequestGenerator.Generate();
	long requestTicks = DateTime.Now.Ticks;
	string correlationId = Guid.NewGuid().ToString();

	await SendOrderRequest(orderRequest, correlationId);
	await TrackOrderRequest(orderRequest, correlationId);

	AnsiConsole.MarkupLine($"[green]Published OrderRequest event[/] [blue]CorrelationId: {correlationId}[/]");
	AnsiConsole.MarkupLine($"\t[grey]OrderId:[/] {orderRequest.OrderId}");
	AnsiConsole.MarkupLine($"\t[grey]CustomerId:[/] {orderRequest.CustomerId}");
	AnsiConsole.MarkupLine($"\t[grey]Request Ticks:[/] {requestTicks}");

	await Task.Delay(2000); // Simulate batch intervals
}

async Task TrackOrderRequest(OrderRequest orderRequest, string correlationId)
{
	OrderRequestTracking entity = new()
	{
		PartitionKey = orderRequest.CustomerId,
		RowKey = correlationId,
		CorrelationId = correlationId,
		CustomerId = orderRequest.CustomerId,
		OrderId = orderRequest.OrderId,
		RequestTicks = DateTime.Now.Ticks,
		Timestamp = DateTimeOffset.UtcNow
	};

	await tableClient.UpsertEntityAsync(entity);
}

async Task SendOrderRequest(OrderRequest orderRequest, string correlationId)
{
	ServiceBusMessage message = new(JsonSerializer.Serialize(orderRequest))
	{
		ContentType = "application/json",
		CorrelationId = correlationId,
		ReplyTo = replyQueueName
	};
	await sender.SendMessageAsync(message);
}