using Bogus;
using OrderPublisher.Models;

namespace OrderPublisher.Services
{
	public static class OrderGenerator
	{
		public static OrderCreated Generate()
		{
			Faker<OrderCreated> faker = new Faker<OrderCreated>()
				.RuleFor(o => o.OrderId, f => Guid.NewGuid().ToString())
				.RuleFor(o => o.ProductName, f => f.Commerce.ProductName())
				.RuleFor(o => o.Quantity, f => f.Random.Int(1, 10))
				.RuleFor(o => o.Price, f => f.Finance.Amount(10, 500))
				.RuleFor(o => o.CreatedAt, f => DateTime.UtcNow)
				.RuleFor(o => o.CorrelationId, _ => Guid.NewGuid().ToString())
				.RuleFor(o => o.Source, _ => "OrderPublisher");

			return faker.Generate();
		}
	}
}