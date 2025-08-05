using System.Text.Json;

namespace SharedLib;

public static class EventSerializer
{
	private static readonly JsonSerializerOptions _options = new()
	{
		PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
		WriteIndented = true
	};

	public static string Serialize<T>(T obj) => JsonSerializer.Serialize(obj, _options);
	public static T Deserialize<T>(string json)
	{
		T? result = JsonSerializer.Deserialize<T>(json, _options);
		return result is null ? throw new InvalidOperationException("Deserialization returned null.") : result;
	}
}