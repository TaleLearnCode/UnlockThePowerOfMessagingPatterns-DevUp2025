using Azure.Messaging.ServiceBus;
using Bogus;
using FulfillmentService.Models;
using MessagingContracts;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Text.Json;

AnsiConsole.Write(new FigletText("Fulfillment Service").Centered().Color(Color.Magenta1));
Console.WriteLine();

IConfigurationRoot config = new ConfigurationBuilder()
		.SetBasePath(Directory.GetCurrentDirectory())
		.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
		.Build();
string serviceBusConnectionString = config["ServiceBus:ConnectionString"]!;
string requestQueueName = config["ServiceBus:RequestQueueName"]!;

ServiceBusClient client = new(serviceBusConnectionString);
ServiceBusProcessor processor = client.CreateProcessor(requestQueueName, new ServiceBusProcessorOptions
{
	AutoCompleteMessages = false,
	MaxConcurrentCalls = 1
});

JsonSerializerOptions jsonSerializeOptions = new()
{
	PropertyNameCaseInsensitive = true
};

processor.ProcessMessageAsync += async args =>
{
	try
	{
		Console.WriteLine();

		if (await IsRequestValidAsync(args))
		{
			OrderRequestMessage orderRequestMessage = ReceiveIncomingMessage(args);
			FulfillmentResponse fulfillmentResponse = await GetFulfillmentResponseAsync(orderRequestMessage);
			await SendFulfillmentResponseAsync(fulfillmentResponse, orderRequestMessage);
			await args.CompleteMessageAsync(args.Message);
		}
	}
	catch (Exception ex)
	{
		await args.AbandonMessageAsync(args.Message);
		AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything);
	}
};

processor.ProcessErrorAsync += args =>
{
	AnsiConsole.MarkupLine($"[red]Error processing message: {args.Exception.Message}[/]");
	return Task.CompletedTask;
};

AnsiConsole.MarkupLine($"[green]Listening on Service Bus queue: {requestQueueName}[/]");
await processor.StartProcessingAsync();

await Task.Delay(-1); // Keep the application running until manually stopped

// ----------------------------------------------------------------------------
// Helper methods
// ----------------------------------------------------------------------------

OrderRequestMessage ReceiveIncomingMessage(ProcessMessageEventArgs args)
{
	OrderRequestMessage orderRequestMessage = new()
	{
		OrderRequest = JsonSerializer.Deserialize<OrderRequest>(args.Message.Body, jsonSerializeOptions)!,
		CorrelationId = args.Message.CorrelationId,
		ReplyTo = args.Message.ReplyTo
	};

	AnsiConsole.MarkupLine("[green]Received OrderRequest event[/]");
	AnsiConsole.MarkupLine($"\t[grey]OrderId:[/] {orderRequestMessage.OrderRequest.OrderId}");
	AnsiConsole.MarkupLine($"\t[grey]CustomerId:[/] {orderRequestMessage.OrderRequest.CustomerId}");
	AnsiConsole.MarkupLine($"\t[grey]CorrelationId:[/] {orderRequestMessage.CorrelationId}");
	AnsiConsole.MarkupLine($"\t[grey]ReplyTo:[/] {orderRequestMessage.ReplyTo}");

	return orderRequestMessage;
}

static async Task<bool> IsRequestValidAsync(ProcessMessageEventArgs args)
{
	return await IsRequestMessageValidAsync(args) &&
		await IsCorrelationIdPresentAsync(args) &&
		await IsReplyToPresentAsync(args);
}

static async Task<bool> IsRequestMessageValidAsync(ProcessMessageEventArgs args)
{
	JsonDocument document = JsonDocument.Parse(args.Message.Body);
	if (!document.RootElement.TryGetProperty("OrderId", out var orderIdProp) ||
			!document.RootElement.TryGetProperty("CustomerId", out var customerIdProp) ||
			orderIdProp.ValueKind != JsonValueKind.String ||
			customerIdProp.ValueKind != JsonValueKind.String)
	{
		AnsiConsole.MarkupLine("[red]Invalid OrderRequest JSON schema.[/]");
		await args.DeadLetterMessageAsync(args.Message, "InvalidSchema", "The OrderRequest message does not conform to the expected schema.");
		return false;
	}
	return true;
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

static async Task<bool> IsReplyToPresentAsync(ProcessMessageEventArgs args)
{
	if (string.IsNullOrWhiteSpace(args.Message.ReplyTo))
	{
		AnsiConsole.MarkupLine("[red]ReplyTo is missing in the message.[/]");
		await args.DeadLetterMessageAsync(args.Message, "MissingReplyTo", "The OrderRequest message is missing a ReplyTo address.");
		return false;
	}
	return true;
}

static async Task<FulfillmentResponse> GetFulfillmentResponseAsync(OrderRequestMessage orderRequestMessage)
{
	await Task.Delay(3000); // Wait three seconds to simulate processing time
	AnsiConsole.MarkupLine($"[yellow]Generating FulfillmentResponse for CorrelationId: {orderRequestMessage.CorrelationId}...[/]");
	return new Faker<FulfillmentResponse>()
		.RuleFor(r => r.OrderId, f => orderRequestMessage.OrderRequest.OrderId)
		.RuleFor(r => r.CustomerId, f => orderRequestMessage.OrderRequest.CustomerId)
		.RuleFor(r => r.IsSuccessful, f => f.Random.Bool(0.9f)) // 90% success rate
		.RuleFor(r => r.ErrorCode, (f, r) => r.IsSuccessful ? null : f.PickRandom("ERR-400", "ERR-500", "ERR-TIMEOUT"))
		.RuleFor(r => r.ErrorMessage, (f, r) => r.IsSuccessful ? null : f.Lorem.Sentence())
		.RuleFor(r => r.IsFulfilled, (f, r) => r.IsSuccessful && f.Random.Bool(0.95f)) // most successful are fulfilled
		.RuleFor(r => r.FulfilledAt, (f, r) => r.IsFulfilled ? f.Date.Recent() : DateTime.MinValue)
		.RuleFor(r => r.FulfilledBy, (f, r) => r.IsFulfilled ? f.Internet.UserName() : null);
}

async Task SendFulfillmentResponseAsync(FulfillmentResponse fulfillmentResponse, OrderRequestMessage orderRequestMessage)
{
	AnsiConsole.MarkupLine($"[green]Sending FulfillmentResponse for CorrelationId: {orderRequestMessage.CorrelationId}...[/]");
	await using ServiceBusSender replySender = client.CreateSender(orderRequestMessage.ReplyTo);
	replySender.SendMessageAsync(new ServiceBusMessage(JsonSerializer.Serialize(fulfillmentResponse))
	{
		ContentType = "application/json",
		CorrelationId = orderRequestMessage.CorrelationId
	}).GetAwaiter().GetResult();
}