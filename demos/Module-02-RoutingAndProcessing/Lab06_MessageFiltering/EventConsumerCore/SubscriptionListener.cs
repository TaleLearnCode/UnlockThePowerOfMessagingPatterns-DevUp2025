using Azure.Messaging.ServiceBus;
using Spectre.Console;

namespace EventConsumerCore;

public class SubscriptionListener(string connectionString, string topicName, string subscriptionName, int maxMessages, int millisecondsDelay)
{

	private readonly string _connectionString = connectionString;
	private readonly string _topicName = topicName;
	private readonly string _subscriptionName = subscriptionName;
	private readonly int _maxMessages = maxMessages;
	private readonly int _millisecondsDelay = millisecondsDelay;

	public async Task StartListeningAsync()
	{
		AnsiConsole.Write(new FigletText(_subscriptionName).Centered().Color(Color.Blue));

		ServiceBusClient client = new(_connectionString);
		ServiceBusProcessor processor = client.CreateProcessor(_topicName, _subscriptionName, new ServiceBusProcessorOptions
		{
			AutoCompleteMessages = false,
			MaxConcurrentCalls = 1
		});

		int messageCount = 0;

		processor.ProcessMessageAsync += async args =>
		{
			ServiceBusReceivedMessage message = args.Message;
			DisplayMessage(message);
			await args.CompleteMessageAsync(message);
			messageCount = await PauseLowerPriorityQueues(messageCount);
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

	private async Task<int> PauseLowerPriorityQueues(int messageCount)
	{
		messageCount++;
		Console.WriteLine();
		AnsiConsole.MarkupLine($"[blue]Processed message count:[/] {messageCount}");
		if (_maxMessages > 0 && messageCount >= _maxMessages)
		{
			AnsiConsole.MarkupLine($"[yellow]Maximum message count reached: {_maxMessages}[/]");
			AnsiConsole.MarkupLine($"[yellow]Pausing message processing for {_millisecondsDelay} milliseconds.[/]");
			await Task.Delay(_millisecondsDelay);
			messageCount = 0;
		}
		return messageCount;
	}

	private static void DisplayMessage(ServiceBusReceivedMessage message)
	{
		AnsiConsole.MarkupLine("[green]Received filtered event:[/]");
		AnsiConsole.MarkupLine($"\t[grey]Priority:[/] {message.ApplicationProperties["priority"]}");
		AnsiConsole.MarkupLine($"\t[grey]Body:[/] {message.Body}");
		AnsiConsole.MarkupLine($"\t[grey]Correlation ID:[/] {message.ApplicationProperties["correlationId"]}");
		AnsiConsole.MarkupLine($"\t[grey]Published At:[/] {message.ApplicationProperties["publishedAt"]}");
	}
}