using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using SharedLib;
using SharedLib.Models;
using Spectre.Console;

AnsiConsole.Write(new FigletText("Observer").Centered().Color(Color.CadetBlue));

IConfiguration config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json")
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string topic = config["ServiceBus:FulfillmentTopic"]!;
string subscription = config["ServiceBus:Subscription"]!;

ServiceBusClient client = new(connectionString);
ServiceBusProcessor processor = client.CreateProcessor(topic, subscription);

processor.ProcessMessageAsync += async args =>
{
	string raw = args.Message.Body.ToString();
	FulfillmentReadyEvent fulfilled = EventSerializer.Deserialize<FulfillmentReadyEvent>(raw);

	AnsiConsole.Write(new Panel($"""
        [green]Fulfillment Completed[/]
        OrderId: [bold]{fulfilled.OrderId}[/]
        CustomerId: [bold]{fulfilled.CustomerId}[/]
        Paid: [bold]{fulfilled.PaymentDetails.Amount:C}[/]
        Items: [bold]{string.Join(", ", fulfilled.InventoryDetails.ItemIds)}[/]
        CorrelationId: [bold]{fulfilled.CorrelationId}[/]
        """).Border(BoxBorder.Rounded).Header("[green]Composite Event Received[/]"));

	await args.CompleteMessageAsync(args.Message);
};

processor.ProcessErrorAsync += e =>
{
	AnsiConsole.MarkupLine($"[red]Observer Error:[/] {e.Exception.Message}");
	return Task.CompletedTask;
};

await processor.StartProcessingAsync();
AnsiConsole.MarkupLine("[gray]Observing FulfillmentReadyEvent messages...[/]");
Console.ReadKey(true);
AnsiConsole.MarkupLine("[red]Shutting down observer.[/]");
await processor.StopProcessingAsync();