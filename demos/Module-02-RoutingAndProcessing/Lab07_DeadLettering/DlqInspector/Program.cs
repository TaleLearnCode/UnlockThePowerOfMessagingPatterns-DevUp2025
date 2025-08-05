using Azure.Messaging.ServiceBus;
using DlqInspector;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using Spectre.Console.Json;
using System.Text.Json;

AnsiConsole.Write(new FigletText("DLQ Inspector").Centered().Color(Color.Green));

// Load configuration
IConfiguration config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json", optional: false)
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string queueName = config["ServiceBus:QueueName"]!;

// Register the Processor
ServiceBusClient client = new(connectionString);
ServiceBusProcessor dlqProcessor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions
{
	AutoCompleteMessages = false,
	MaxConcurrentCalls = 5,
	SubQueue = SubQueue.DeadLetter
});

// Add a sender to attempt remediation
ServiceBusSender remediationSender = client.CreateSender(queueName);

// Message Inspection Logic
dlqProcessor.ProcessMessageAsync += async args =>
{
	ServiceBusReceivedMessage msg = args.Message;

	Console.WriteLine();

	// Create a JSON panel for the message body
	JsonText messageBody = new(msg.Body.ToString());
	Panel jsonPanel = new Panel(messageBody)
		.Header("Message Body")
		.Collapse()
		.RoundedBorder()
		.BorderColor(Color.Yellow);

	// Compose all DLQ message details
	Markup messageDetails = new($"[bold red]Dead Letter Queue Message Detected:[/]\n" +
															$"[bold gray]Message ID:[/] {msg.MessageId}\n" +
															$"[bold gray]Subject:[/] {msg.Subject}\n" +
															$"[bold gray]Dead Letter Reason:[/] {msg.DeadLetterReason}\n" +
															$"[bold gray]Dead Letter Error Description:[/] {msg.DeadLetterErrorDescription}");

	// Create the outer panel with inner JSON panel
	Panel outerPanel = new Panel(new Rows([messageDetails, jsonPanel]))
		.Header($"DLQ Message: {msg.MessageId}")
		.RoundedBorder()
		.BorderColor(Color.Red);

	AnsiConsole.Write(outerPanel);

	// Attempt remediation if the message body is JSON
	if (msg.DeadLetterReason == "DeserializationFailure")
	{
		Console.WriteLine();
		AnsiConsole.MarkupLine("[orange1 bold]Attempting remediation for deserialization failure...[/]");
		try
		{
			string jsonBody = msg.Body.ToString();
			object? fixedMessage = RemediationStrategy.AttemptFix(jsonBody);
			if (fixedMessage is not null)
			{
				// Send the fixed message back to the original queue
				ServiceBusMessage remediationMessage = new(JsonSerializer.Serialize(fixedMessage))
				{
					MessageId = msg.MessageId, // Preserve original message ID
					Subject = msg.Subject // Preserve original subject
				};
				remediationMessage.ApplicationProperties["OriginalMessageId"] = msg.MessageId; // Track original message ID
				remediationMessage.ApplicationProperties["RemediationApplied"] = true; // Mark as remediated
				remediationMessage.ApplicationProperties["RemediationContext"] = "QuantityFieldNormalized";
				remediationMessage.CorrelationId = msg.CorrelationId; // Preserve correlation ID
				await remediationSender.SendMessageAsync(remediationMessage);
				AnsiConsole.MarkupLine("[green]Remediation successful. Message sent back to the original queue.[/]");
			}
			else
			{
				AnsiConsole.MarkupLine("[red]Failed to remediate the message. No valid fix found.[/]");
			}
		}
		catch (Exception ex)
		{
			AnsiConsole.MarkupLine($"[red]Error during remediation: {ex.Message}[/]");
		}
	}

	await args.CompleteMessageAsync(msg);
};

// Error handler
dlqProcessor.ProcessErrorAsync += args =>
{
	AnsiConsole.MarkupLine($"[red bold]Processor error:[/] {args.Exception.Message}");
	return Task.CompletedTask;
};

// Start the processor
await dlqProcessor.StartProcessingAsync();
AnsiConsole.MarkupLine("[yellow]DLQ Inspector running. Press Ctrl+C to stop...[/]");

// Handle graceful shutdown
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

// Stop and dispose the processor and client
await dlqProcessor.StopProcessingAsync();
await dlqProcessor.DisposeAsync();
await client.DisposeAsync();

// Notify shutdown
AnsiConsole.MarkupLine("[green]DLQ Inspector shut down.[/]");