using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace FastServiceBusEmulator.Aspire.Hosting;

/// <summary>
/// A client for the Fast Service Bus Emulator Admin API, used for testing purposes.
/// </summary>
public class TestingClient(HttpClient httpClient)
{
    /// <summary>
    /// Sends a message to a queue through the testing REST API.
    /// </summary>
    public async Task PostQueueMessageAsync(string queueName, MessageRequest message, CancellationToken cancellationToken = default)
    {
        using var request = BuildPostRequest($"/testing/messages/queues/{queueName}", message);
        var response = await httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Sends a message to a topic through the testing REST API.
    /// </summary>
    public async Task PostTopicMessageAsync(string topicName, MessageRequest message, CancellationToken cancellationToken = default)
    {
        using var request = BuildPostRequest($"/testing/messages/topics/{topicName}", message);
        var response = await httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

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

    private static HttpRequestMessage BuildPostRequest(string path, MessageRequest message)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, path);

        if (message.MessageId is not null)
        {
            request.Headers.Add("x-message-message-id", message.MessageId);
        }

        if (message.Subject is not null)
        {
            request.Headers.Add("x-message-subject", message.Subject);
        }

        if (message.UserId is not null)
        {
            request.Headers.Add("x-message-user-id", message.UserId);
        }

        if (message.To is not null)
        {
            request.Headers.Add("x-message-to", message.To);
        }

        if (message.ReplyTo is not null)
        {
            request.Headers.Add("x-message-reply-to", message.ReplyTo);
        }

        if (message.CorrelationId is not null)
        {
            request.Headers.Add("x-message-correlation-id", message.CorrelationId);
        }

        if (message.ContentType is not null)
        {
            request.Headers.Add("x-message-content-type", message.ContentType);
        }

        if (message.GroupId is not null)
        {
            request.Headers.Add("x-message-group-id", message.GroupId);
        }

        if (message.ReplyToGroupId is not null)
        {
            request.Headers.Add("x-message-reply-to-group-id", message.ReplyToGroupId);
        }

        if (message.AbsoluteExpiration.HasValue)
        {
            request.Headers.Add("x-message-absolute-expiry-time", message.AbsoluteExpiration.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
        }

        foreach (var (key, value) in message.AppProperties)
        {
            request.Headers.Add("x-message-property", $"{key}={value}");
        }

        request.Content = message switch
        {
            BinaryMessageRequest { Content: not null } br => new ByteArrayContent(br.Content),
            StringMessageRequest { Content: not null } str => new StringContent(str.Content),
            _ => null
        };

        return request;
    }
}

public abstract class MessageRequest
{
    public string? MessageId { get; set; }
    public string? Subject { get; set; }
    public string? UserId { get; set; }
    public string? To { get; set; }
    public string? ReplyTo { get; set; }
    public string? CorrelationId { get; set; }
    public string? ContentType { get; set; }
    public string? GroupId { get; set; }
    public string? ReplyToGroupId { get; set; }
    public DateTimeOffset? AbsoluteExpiration { get; set; }
    public Dictionary<string, string> AppProperties { get; set; } = new();
}

public class BinaryMessageRequest: MessageRequest
{
    public byte[]? Content { get; set; }
}

public class StringMessageRequest : MessageRequest
{
    public string? Content { get; set; }
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
