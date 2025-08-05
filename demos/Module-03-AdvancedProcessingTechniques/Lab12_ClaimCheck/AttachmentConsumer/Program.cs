using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Text.Json;

AnsiConsole.Write(new FigletText("ClaimCheck Consumer").Centered().Color(Color.Aqua));

IConfigurationRoot config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json")
	.Build();

string blobConnection = config["BlobStorage:ConnectionString"]!;
string containerName = config["BlobStorage:ContainerName"]!;
string serviceBusConnection = config["ServiceBus:ConnectionString"]!;
string queueName = config["ServiceBus:QueueName"]!;

BlobContainerClient containerClient = new(blobConnection, containerName);
ServiceBusClient sbClient = new(serviceBusConnection);
ServiceBusProcessor processor = sbClient.CreateProcessor(queueName, new ServiceBusProcessorOptions
{
	MaxConcurrentCalls = 1,
	AutoCompleteMessages = true
});

processor.ProcessMessageAsync += async args =>
{
	string body = args.Message.Body.ToString();
	ClaimMessage claim = JsonSerializer.Deserialize<ClaimMessage>(body)!;

	AnsiConsole.MarkupLine($"[blue]Received Claim:[/] [bold]{claim.AttachmentId}[/]");

	BlobClient blobClient = containerClient.GetBlobClient(claim.AttachmentId);
	Azure.Response<Azure.Storage.Blobs.Models.BlobDownloadResult> blobContent = await blobClient.DownloadContentAsync();

	AnsiConsole.Write(
		new Panel($"[green]Blob Retrieved[/]\nContent:\n[italic]{blobContent.Value.Content}[/]")
			.Header("[green]Processed Attachment[/]")
			.Border(BoxBorder.Rounded)
	);
};

processor.ProcessErrorAsync += args =>
{
	AnsiConsole.MarkupLine($"[red]Error:[/] {args.Exception.Message}");
	return Task.CompletedTask;
};

await processor.StartProcessingAsync();

Console.WriteLine("Listening for claim check messages. Press any key to exit...");
Console.ReadKey();
await processor.StopProcessingAsync();

record ClaimMessage(string AttachmentId, string BlobUri, string SourceSystem, DateTime Timestamp);