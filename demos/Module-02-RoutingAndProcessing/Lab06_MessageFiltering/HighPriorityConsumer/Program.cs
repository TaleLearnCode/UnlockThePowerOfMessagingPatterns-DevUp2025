using EventConsumerCore;
using Microsoft.Extensions.Configuration;

IConfigurationRoot config = new ConfigurationBuilder()
	.AddJsonFile("appsettings.json")
	.Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string topicName = config["ServiceBus:TopicName"]!;
string subscriptionName = config["ServiceBus:SubscriptionName"]!;

if (!int.TryParse(config["PriorityQueueHandling:MaxMessagesPerSession"], out int maxMessages))
{
	maxMessages = 5; // Default to 5 if not set or invalid
}
if (!int.TryParse(config["PriorityQueueHandling:MillisecondsDelay"], out int millisecondsDelay))
{
	millisecondsDelay = 5000; // Default to 5000 ms if not set or invalid
}

SubscriptionListener listener = new(connectionString, topicName, subscriptionName, maxMessages, millisecondsDelay);
await listener.StartListeningAsync();