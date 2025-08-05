using MessagingContracts;

namespace OrderClient.Services;

internal static class OrderRequestGenerator
{
	internal static OrderRequest Generate() => new Bogus.Faker<OrderRequest>()
		.RuleFor(o => o.OrderId, f => $"ORD-{f.Random.Int(10000, 99999)}")
		.RuleFor(o => o.CustomerId, f => $"CUST-{f.Random.Int(10000, 99999)}");
}
