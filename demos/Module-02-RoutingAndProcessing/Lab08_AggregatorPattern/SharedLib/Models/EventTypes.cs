namespace SharedLib.Models;

public static class EventTypes
{
	public const string OrderCreated = "OrderCreated";
	public const string PaymentConfirmed = "PaymentConfirmed";
	public const string InventoryReserved = "InventoryReserved";
	public const string FulfillmentReady = "FulfillmentReady";
}