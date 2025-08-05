using Azure.Data.Tables;
using Azure.Messaging.ServiceBus;
using MessagingContracts;
using Microsoft.Extensions.Configuration;
using Persistence.Models;
using Spectre.Console;
using System.Text.Json;

AnsiConsole.Write(new FigletText("Reply Handler").Centered().Color(Color.Cyan1));
Console.WriteLine();

JsonSerializerOptions jsonSerializeOptions = new()
{
	PropertyNameCaseInsensitive = true
};

IConfigurationRoot config = new ConfigurationBuilder()
		.SetBasePath(Directory.GetCurrentDirectory())
		.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
		.Build();
string serviceBusConnectionString = config["ServiceBus:ConnectionString"]!;
string replyQueueName = config["ServiceBus:ReplyQueueName"]!;
string storageAccountConnectionString = config["Storage:ConnectionString"]!;
string trackingTableName = config["Storage:TrackingTableName"]!;

TableClient tableClient = new(storageAccountConnectionString, trackingTableName);
await tableClient.CreateIfNotExistsAsync();

ServiceBusClient client = new(serviceBusConnectionString);
ServiceBusProcessor processor = client.CreateProcessor(replyQueueName, new ServiceBusProcessorOptions
{
	AutoCompleteMessages = false,
	MaxConcurrentCalls = 1
});

processor.ProcessMessageAsync += async args =>
{

	long responseTicks = DateTime.Now.Ticks;

	Console.WriteLine();
	AnsiConsole.MarkupLine("[green]Processing FulfillmentResponse message...[/]");

	if (!await IsResponseValid(args))
	{
		await args.DeadLetterMessageAsync(args.Message, "InvalidMessage", "The FulfillmentResponse message is invalid or missing required properties.");
		return;
	}
	FulfillmentResponse fulfillmentResponse = JsonSerializer.Deserialize<FulfillmentResponse>(args.Message.Body, jsonSerializeOptions)!;

	OrderRequestTracking? orderRequestTracking = await GetOrderRequestTrackingAsync(fulfillmentResponse.CustomerId, args.Message.CorrelationId);
	if (orderRequestTracking is null)
	{
		await args.DeadLetterMessageAsync(args.Message, "TrackingNotFound", "No tracking information found for the provided CorrelationId.");
		return;
	}

	await PersistResponseDetailsAsync(tableClient, responseTicks, fulfillmentResponse, orderRequestTracking);
	DisplayResponse(args, fulfillmentResponse, orderRequestTracking);

	await args.CompleteMessageAsync(args.Message);

};

processor.ProcessErrorAsync += args =>
{
	AnsiConsole.MarkupLine($"[red]Error processing message: {args.Exception.Message}[/]");
	return Task.CompletedTask;
};

AnsiConsole.MarkupLine($"[green]Listening for FulfillmentResponse messages on queue: {replyQueueName}[/]");
await processor.StartProcessingAsync();

await Task.Delay(-1); // Keep the application running until manually stopped

/// ---------------------------------------------------------------------------
/// Helper Methods
/// ----------------------------------------------------------------------------

static async Task<bool> IsResponseValid(ProcessMessageEventArgs args)
{
	return await IsCorrelationIdPresentAsync(args)
		&& await IsResponseMessageValidAsync(args);
}

static async Task<bool> IsCorrelationIdPresentAsync(ProcessMessageEventArgs args)
{
	if (string.IsNullOrWhiteSpace(args.Message.CorrelationId))
	{
		AnsiConsole.MarkupLine("[red]CorrelationId is missing in the message.[/]");
		await args.DeadLetterMessageAsync(args.Message, "MissingCorrelationId", "The OrderRequest message is missing a CorrelationId.");
		return false;
	}
	return true;
}

static async Task<bool> IsResponseMessageValidAsync(ProcessMessageEventArgs args)
{
	JsonDocument document = JsonDocument.Parse(args.Message.Body);

	if (!document.RootElement.TryGetProperty("OrderId", out JsonElement orderIdProp) ||
			!document.RootElement.TryGetProperty("CustomerId", out JsonElement customerIdProp) ||
			!document.RootElement.TryGetProperty("IsSuccessful", out JsonElement isSuccessfulProp) ||
			!document.RootElement.TryGetProperty("IsFulfilled", out JsonElement isFulfilledProp) ||
			!document.RootElement.TryGetProperty("FulfilledAt", out JsonElement fulfilledAtProp) ||
			orderIdProp.ValueKind != JsonValueKind.String ||
			customerIdProp.ValueKind != JsonValueKind.String ||
			isSuccessfulProp.ValueKind != JsonValueKind.True && isSuccessfulProp.ValueKind != JsonValueKind.False ||
			isFulfilledProp.ValueKind != JsonValueKind.True && isFulfilledProp.ValueKind != JsonValueKind.False ||
			fulfilledAtProp.ValueKind != JsonValueKind.String) // DateTime is serialized as string
	{
		AnsiConsole.MarkupLine("[red]Invalid FulfillmentResponse JSON schema.[/]");
		await args.DeadLetterMessageAsync(args.Message, "InvalidSchema", "The FulfillmentResponse message does not conform to the expected schema.");
		return false;
	}

	return true;
}

async Task<OrderRequestTracking?> GetOrderRequestTrackingAsync(string customerId, string correlationId)
{
	Azure.NullableResponse<TableEntity> nullableEntity = await tableClient.GetEntityIfExistsAsync<TableEntity>(customerId, correlationId);
	if (nullableEntity is null || !nullableEntity.HasValue || nullableEntity.Value is null)
	{
		AnsiConsole.MarkupLine("[red]No tracking information found for CorrelationId: {0}[/]", correlationId);
		return null;
	}
	else
	{
		return new OrderRequestTracking
		{
			PartitionKey = nullableEntity.Value.PartitionKey,
			RowKey = nullableEntity.Value.RowKey,
			CorrelationId = nullableEntity.Value.GetString("CorrelationId"),
			OrderId = nullableEntity.Value.GetString("OrderId"),
			CustomerId = nullableEntity.Value.GetString("CustomerId"),
			RequestTicks = nullableEntity.Value.GetInt64("RequestTicks") ?? 0,
			ResponseTicks = nullableEntity.Value.GetInt64("ResponseTicks") ?? 0,
			Timestamp = nullableEntity.Value.Timestamp,
			ETag = nullableEntity.Value.ETag
		};
	}
}

static void DisplayResponse(ProcessMessageEventArgs args, FulfillmentResponse fulfillmentResponse, OrderRequestTracking orderRequestTracking)
{
	TimeSpan timeTaken = TimeSpan.FromTicks(orderRequestTracking.ResponseTicks - orderRequestTracking.RequestTicks);
	AnsiConsole.MarkupLine($"[green]Fulfillment Response received:[/] [blue]CorrelationId: {args.Message.CorrelationId}[/]");
	AnsiConsole.MarkupLine($"\t[grey]Order Id:[/] {fulfillmentResponse.OrderId}");
	AnsiConsole.MarkupLine($"\t[grey]Customer Id:[/] {fulfillmentResponse.CustomerId}");
	AnsiConsole.MarkupLine($"\t[grey]Is Successful:[/] {fulfillmentResponse.IsSuccessful}");
	AnsiConsole.MarkupLine($"\t[grey]Time Taken:[/] {timeTaken.TotalMilliseconds} milliseconds");
	if (fulfillmentResponse.IsSuccessful)
	{
		AnsiConsole.MarkupLine($"\t[grey]Is Fulfilled:[/] {fulfillmentResponse.IsFulfilled}");
		if (fulfillmentResponse.IsFulfilled)
		{
			AnsiConsole.MarkupLine($"\t[green]Order Fulfilled![/]");
			AnsiConsole.MarkupLine($"\t\t[grey]Fulfilled At:[/] {fulfillmentResponse.FulfilledAt}");
			AnsiConsole.MarkupLine($"\t\t[grey]Fulfilled By:[/] {fulfillmentResponse.FulfilledBy}");
		}
		else
		{
			AnsiConsole.MarkupLine("\t[yellow]Order is not fulfilled yet.[/]");
		}
	}
	else
	{
		AnsiConsole.MarkupLine($"\t[red]Request failed![/]");
		AnsiConsole.MarkupLine($"\t\t[grey]Error Code:[/] {fulfillmentResponse.ErrorCode}");
		AnsiConsole.MarkupLine($"\t\t[grey]Error Message:[/] {fulfillmentResponse.ErrorMessage}");
	}
}

static async Task PersistResponseDetailsAsync(TableClient tableClient, long responseTicks, FulfillmentResponse fulfillmentResponse, OrderRequestTracking orderRequestTracking)
{
	orderRequestTracking.IsSuccessful = fulfillmentResponse.IsSuccessful;
	orderRequestTracking.ResponseTicks = responseTicks;
	await tableClient.UpdateEntityAsync(orderRequestTracking, orderRequestTracking.ETag, TableUpdateMode.Replace);
}