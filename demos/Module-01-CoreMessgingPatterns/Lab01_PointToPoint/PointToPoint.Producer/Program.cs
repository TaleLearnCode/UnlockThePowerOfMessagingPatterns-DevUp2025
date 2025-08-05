/// <summary>
/// This application sends sample order messages to an Azure Service Bus queue.
/// Configuration is loaded from appsettings.json for connection string and queue name.
/// The user can send multiple messages interactively until 'q' is pressed.
/// </summary>
using Azure.Messaging.ServiceBus;
using Bogus;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using Spectre.Console.Json;
using System.Text.Json;


try
{

	// Display application banner
	AnsiConsole.Write(new FigletText("Point to Point Producer").Centered().Color(Color.Green));

	// Load configuration from appsettings.json
	IConfigurationRoot config = new ConfigurationBuilder()
		.AddJsonFile("appsettings.json")
		.Build();

	string connectionString = config["ServiceBus:ConnectionString"]!;
	string queueName = config["ServiceBus:QueueName"]!;

	// Create Service Bus client and sender
	await using ServiceBusClient client = new(connectionString);
	ServiceBusSender sender = client.CreateSender(queueName);

	// Define a Faker for generating sample order data
	string[] fruit = ["Apple", "Banana", "Cherry", "Date", "Elderberry"];
	Faker<OrderDetails> testOrders = new Faker<OrderDetails>()
		.StrictMode(true)
		.RuleFor(o => o.OrderId, f => Guid.NewGuid())
		.RuleFor(o => o.Item, f => f.PickRandom(fruit))
		.RuleFor(o => o.Quantity, f => f.Random.Number(1, 10));

	// Prompt user to start sending messages
	Console.WriteLine();
	Console.WriteLine("Press any key to start sending messages to the queue...");
	Console.ReadKey(true);

	do
	{

		// Create sample order payload
		OrderDetails order = testOrders.Generate();
		string jsonPayload = JsonSerializer.Serialize(order);

		// Create and send Service Bus message
		ServiceBusMessage message = new(jsonPayload)
		{
			MessageId = order.OrderId.ToString()
		};
		await sender.SendMessageAsync(message);

		// Display message details in a formatted panel
		AnsiConsole.Write(new Panel(new JsonText(jsonPayload))
			.Header("Message Sent")
			.Collapse()
			.RoundedBorder()
			.BorderColor(Color.Yellow));

		// Prompt user for next action
		Console.WriteLine();
		Console.WriteLine("Press any key to send another message, or 'q' to quit.");

	} while (Console.ReadKey(true).Key != ConsoleKey.Q);

}
catch (Exception ex)
{
	// Display exception details in a formatted way
	AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything | ExceptionFormats.ShowLinks);
}

/// <summary>
/// Represents the details of an order, including the order identifier, item name, and quantity.
/// </summary>
internal class OrderDetails
{
	public Guid OrderId { get; set; }
	public string Item { get; set; } = string.Empty;
	public int Quantity { get; set; }
}