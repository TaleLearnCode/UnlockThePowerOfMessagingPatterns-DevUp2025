using Azure.Messaging.ServiceBus;
using Spectre.Console;

namespace EventConsumerCore;

public class SubscriptionListener(string connectionString, string topicName, string subscriptionName)
{

	private readonly string _connectionString = connectionString;
	private readonly string _topicName = topicName;
	private readonly string _subscriptionName = subscriptionName;

	public async Task StartListeningAsync()
	{
		AnsiConsole.Write(new FigletText(_subscriptionName).LeftJustified().Color(Color.Blue));

		ServiceBusClient client = new(_connectionString);
		ServiceBusProcessor processor = client.CreateProcessor(_topicName, _subscriptionName, new ServiceBusProcessorOptions
		{
			AutoCompleteMessages = false,
			MaxConcurrentCalls = 1
		});

		processor.ProcessMessageAsync += async args =>
		{
			ServiceBusReceivedMessage message = args.Message;

			AnsiConsole.MarkupLine("[green]Received routed event:[/]");
			AnsiConsole.MarkupLine($"\t[grey]Tenant:[/] {message.ApplicationProperties["tenant"]}");
			AnsiConsole.MarkupLine($"\t[grey]Event Type:[/] {message.ApplicationProperties["eventType"]}");
			AnsiConsole.MarkupLine($"\t[grey]Body:[/] {message.Body}");
			AnsiConsole.MarkupLine($"\t[grey]Correlation ID:[/] {message.ApplicationProperties["correlationId"]}");
			AnsiConsole.MarkupLine($"\t[grey]Published At:[/] {message.ApplicationProperties["publishedAt"]}");

			await args.CompleteMessageAsync(message);
		};

		processor.ProcessErrorAsync += args =>
		{
			AnsiConsole.MarkupLine($"[red]Error:[/] {args.Exception.Message}");
			return Task.CompletedTask;
		};

		AnsiConsole.MarkupLine($"[green]Listening on subscription:[/] {_subscriptionName}");
		await processor.StartProcessingAsync();
		await Task.Delay(-1);
	}
}