using System.Text.Json;

namespace DlqInspector;

public static class RemediationStrategy
{
	public static object? AttemptFix(string json)
	{
		try
		{
			JsonElement root = JsonDocument.Parse(json).RootElement;

			if (!root.TryGetProperty("Items", out JsonElement itemsElement) || itemsElement.ValueKind != JsonValueKind.Array)
				return null;
			List<Dictionary<string, object>> items = new List<Dictionary<string, object>>();

			foreach (JsonElement item in root.GetProperty("Items").EnumerateArray())
			{
				Dictionary<string, object> fixedItem = new Dictionary<string, object>
				{
					["Sku"] = item.GetProperty("Sku").GetString() ?? "UNKNOWN"
				};

				JsonElement qtyElement = item.GetProperty("Quantity");
				if (qtyElement.ValueKind == JsonValueKind.String)
				{
					if (!int.TryParse(qtyElement.GetString(), out int qty))
					{
						// If it's a word like "one", convert it to a number
						qty = WordToNumber(qtyElement.GetString()!);
					}
					fixedItem["Quantity"] = qty;
				}
				else
				{
					fixedItem["Quantity"] = qtyElement.GetInt32();
				}

				items.Add(fixedItem);
			}

			Dictionary<string, object> corrected = new Dictionary<string, object>
			{
				["OrderId"] = root.GetProperty("OrderId").GetString()!,
				["CustomerId"] = root.GetProperty("CustomerId").GetString()!,
				["Items"] = items,
				["TotalAmount"] = root.GetProperty("TotalAmount").GetDecimal(),
				["Timestamp"] = root.GetProperty("Timestamp").GetString()!
			};

			return corrected;
		}
		catch
		{
			return null;
		}
	}

	static int WordToNumber(string word)
	{
		Dictionary<string, int> numberWords = new()
		{
						{ "zero", 0 },
						{ "one", 1 },
						{ "two", 2 },
						{ "three", 3 },
						{ "four", 4 },
						{ "five", 5 },
						{ "six", 6 },
						{ "seven", 7 },
						{ "eight", 8 },
						{ "nine", 9 },
						{ "ten", 10 }
				};

		return numberWords.TryGetValue(word.ToLower(), out int value)
				? value
				: throw new ArgumentException($"Unrecognized number word: {word}");
	}
}