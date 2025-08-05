using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using SharedLib;
using SharedLib.Models;
using Spectre.Console;

AnsiConsole.Write(new FigletText("Payments").Centered().Color(Color.Blue));

IConfiguration config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json").Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string ordersTopic = config["ServiceBus:OrdersTopic"]!;
string paymentsTopic = config["ServiceBus:PaymentsTopic"]!;
string subscription = config["ServiceBus:Subscription"]!;
int delayMs = config.GetValue("Processing:DelayMilliseconds", 1000);
string sourceSystem = "PaymentProducer";

ServiceBusClient client = new(connectionString);
ServiceBusProcessor processor = client.CreateProcessor(ordersTopic, subscription);
ServiceBusSender sender = client.CreateSender(paymentsTopic);

processor.ProcessMessageAsync += async args =>
{
	string body = args.Message.Body.ToString();
	OrderCreatedEvent order = EventSerializer.Deserialize<OrderCreatedEvent>(body);

	await Task.Delay(delayMs); // Simulate payment logic

	PaymentConfirmedEvent payment = new()
	{
		PaymentId = $"PAY-{Guid.NewGuid()}",
		OrderId = order.OrderId,
		CustomerId = order.CustomerId,
		Amount = order.Price,
		Timestamp = DateTime.UtcNow,
		CorrelationId = order.CorrelationId,
		SourceSystem = sourceSystem,
		EventType = EventTypes.PaymentConfirmed
	};

	string json = EventSerializer.Serialize(payment);

	ServiceBusMessage msg = new(json)
	{
		MessageId = payment.PaymentId,
		CorrelationId = payment.CorrelationId,
		ContentType = "application/json",
		Subject = EventTypes.PaymentConfirmed,
		ApplicationProperties =
		{
			["eventType"] = EventTypes.PaymentConfirmed,
			["origin"] = sourceSystem
		}
	};

	await sender.SendMessageAsync(msg);
	AnsiConsole.MarkupLine($"[green]Processed Payment for Order:[/] [bold]{order.OrderId}[/]");
	await args.CompleteMessageAsync(args.Message);
};

processor.ProcessErrorAsync += e =>
{
	AnsiConsole.MarkupLine($"[red]Error:[/] {e.Exception.Message}");
	return Task.CompletedTask;
};

await processor.StartProcessingAsync();
AnsiConsole.MarkupLine("[gray]Listening for new orders...[/]");
Console.ReadKey(true);
AnsiConsole.MarkupLine("[red]Shutting down.[/]");
await processor.StopProcessingAsync();