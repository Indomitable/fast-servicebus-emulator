using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace FastServiceBusEmulator.Aspire.Hosting;

/// <summary>
/// A client for the Fast Service Bus Emulator Admin API, used for testing purposes.
/// </summary>
public class TestingClient(HttpClient httpClient)
{
    /// <summary>
    /// Deletes all messages from all queues and subscriptions.
    /// </summary>
    public async Task DeleteAllMessagesAsync(CancellationToken cancellationToken = default)
    {
        var response = await httpClient.DeleteAsync("/testing/messages", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Gets all messages from all queues and subscriptions.
    /// </summary>
    public async Task<List<MessageResponse>> GetAllMessagesAsync(CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync<List<MessageResponse>>("/testing/messages", cancellationToken) ?? [];
    }

    /// <summary>
    /// Deletes all messages from a specific queue.
    /// </summary>
    public async Task DeleteQueueMessagesAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var response = await httpClient.DeleteAsync($"/testing/messages/queues/{queueName}", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Gets messages from a specific queue.
    /// </summary>
    public async Task<List<MessageResponse>> GetQueueMessagesAsync(string queueName, CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync<List<MessageResponse>>($"/testing/messages/queues/{queueName}", cancellationToken) ?? [];
    }

    /// <summary>
    /// Deletes all messages from all subscriptions of a topic.
    /// </summary>
    public async Task DeleteTopicMessagesAsync(string topicName, CancellationToken cancellationToken = default)
    {
        var response = await httpClient.DeleteAsync($"/testing/messages/topics/{topicName}", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Gets messages from all subscriptions of a topic (union of all messages).
    /// </summary>
    public async Task<List<TopicMessageResponse>> GetTopicMessagesAsync(string topicName, CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync<List<TopicMessageResponse>>($"/testing/messages/topics/{topicName}", cancellationToken) ?? [];
    }

    /// <summary>
    /// Deletes messages from a specific subscription.
    /// </summary>
    public async Task DeleteSubscriptionMessagesAsync(string topicName, string subscriptionName, CancellationToken cancellationToken = default)
    {
        var response = await httpClient.DeleteAsync($"/testing/messages/topics/{topicName}/subscriptions/{subscriptionName}", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Gets messages from a specific subscription.
    /// </summary>
    public async Task<List<MessageResponse>> GetSubscriptionMessagesAsync(string topicName, string subscriptionName, CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync<List<MessageResponse>>($"/testing/messages/topics/{topicName}/subscriptions/{subscriptionName}", cancellationToken) ?? [];
    }
}

public class MessageResponse
{
    [JsonPropertyName("message")]
    public Envelope Message { get; set; } = new();

    [JsonPropertyName("dlq")]
    public bool Dlq { get; set; }
    
    [JsonPropertyName("entity")]
    public string? Entity { get; set; }
}

public class TopicMessageResponse
{
    [JsonPropertyName("subscription")]
    public string Subscription { get; set; } = string.Empty;

    [JsonPropertyName("message")]
    public Envelope Message { get; set; } = new();

    [JsonPropertyName("dlq")]
    public bool Dlq { get; set; }
}

public class Envelope
{
    [JsonPropertyName("sequence_number")]
    public ulong SequenceNumber { get; set; }

    [JsonPropertyName("delivery_count")]
    public uint DeliveryCount { get; set; }

    [JsonPropertyName("enqueued_time_utc")]
    public ulong EnqueuedTimeUtc { get; set; }

    [JsonPropertyName("state")]
    public object? State { get; set; }

    [JsonPropertyName("message_debug")]
    public string? MessageDebug { get; set; }
}
