using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Text.Json;

AnsiConsole.Write(new FigletText("Claim Check Producer").Centered().Color(Color.LightGreen));

IConfigurationRoot config = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json")
	.Build();

string blobConnection = config["BlobStorage:ConnectionString"]!;
string containerName = config["BlobStorage:ContainerName"]!;
string serviceBusConnection = config["ServiceBus:ConnectionString"]!;
string queueName = config["ServiceBus:QueueName"]!;
int delayMilliseconds = config.GetValue<int>("Sending:DelayMilliseconds", 3000);
string sourceSystem = "AttachmentProducer";

BlobContainerClient containerClient = new(blobConnection, containerName);
await containerClient.CreateIfNotExistsAsync();

ServiceBusClient serviceBusClient = new(serviceBusConnection);
ServiceBusSender sevriceBusSender = serviceBusClient.CreateSender(queueName);

Console.WriteLine("Press any key to start sending claim messages...");
Console.ReadKey(true);

using CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
	e.Cancel = true;
	cts.Cancel();
	AnsiConsole.MarkupLine("[red]Cancellation requested...[/]");
};

int counter = 1;
while (!cts.Token.IsCancellationRequested)
{
	string blobName = $"attachment-{Guid.NewGuid()}.txt";
	string blobContent = $"This is payload #{counter} from {sourceSystem}.";
	using MemoryStream contentStream = new(System.Text.Encoding.UTF8.GetBytes(blobContent));

	BlobClient blobClient = containerClient.GetBlobClient(blobName);
	await blobClient.UploadAsync(contentStream, overwrite: true, cts.Token);

	ClaimMessage claimMessage = new(blobName, blobClient.Uri.ToString(), sourceSystem, DateTime.UtcNow);

	string json = JsonSerializer.Serialize(claimMessage);
	ServiceBusMessage msg = new(json)
	{
		ContentType = "application/json",
		MessageId = Guid.NewGuid().ToString(),
		CorrelationId = claimMessage.AttachmentId,
		Subject = "AttachmentClaimCheck",
		ApplicationProperties = {
			["eventType"] = "ClaimCheckAttachment",
			["origin"] = sourceSystem
		}
	};

	await sevriceBusSender.SendMessageAsync(msg, cts.Token);

	AnsiConsole.Write(
		new Panel($"[green]Attachment Uploaded[/]\nBlob: [bold]{blobName}[/]\nURI: [blue]{blobClient.Uri}[/]")
			.Header("[green]Claim Message Sent[/]")
			.Border(BoxBorder.Rounded)
	);

	counter++;

	try { await Task.Delay(delayMilliseconds, cts.Token); }
	catch (TaskCanceledException) { break; }
}

internal record ClaimMessage(string AttachmentId, string BlobUri, string SourceSystem, DateTime Timestamp);