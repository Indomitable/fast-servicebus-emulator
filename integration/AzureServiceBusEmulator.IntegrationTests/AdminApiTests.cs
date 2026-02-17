using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

public class AdminApiTests : BaseServiceBusTest
{
    [Fact]
    public async Task ClearQueue_DeletesMessages_And_Get_ReturnsEmpty()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        var sender = client.CreateSender(QueueName);

        // Send 2 messages
        await sender.SendMessageAsync(new ServiceBusMessage("msg1"));
        await sender.SendMessageAsync(new ServiceBusMessage("msg2"));

        // Verify GET returns 2 messages
        var getResp = await AdminHttpClient.GetFromJsonAsync<List<AdminMessageResponse>>($"/testing/messages/queues/{QueueName}");
        Assert.NotNull(getResp);
        Assert.Equal(2, getResp.Count);

        // Delete messages
        var delResp = await AdminHttpClient.DeleteAsync($"/testing/messages/queues/{QueueName}");
        Assert.True(delResp.IsSuccessStatusCode);

        // Verify GET returns 0 messages
        getResp = await AdminHttpClient.GetFromJsonAsync<List<AdminMessageResponse>>($"/testing/messages/queues/{QueueName}");
        Assert.NotNull(getResp);
        Assert.Empty(getResp);

        // Verify Receive returns nothing
        var receiver = client.CreateReceiver(QueueName, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(msg);
    }

    [Fact]
    public async Task GetTopicMessages_ReturnsUnionOfSubscriptions()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        var sender = client.CreateSender(TopicName); // events-topic has sub-1 and sub-2

        // Send 1 message to topic -> Fanout to sub-1 (1 msg) and sub-2 (1 msg) => Total 2
        await sender.SendMessageAsync(new ServiceBusMessage("topic-msg-1"));

        // Verify state:
        // sub-1: 1 message
        // sub-2: 1 message
        // Total in topic: 2 messages (Union of all subscriptions)

        var getResp = await AdminHttpClient.GetFromJsonAsync<List<AdminTopicMessageResponse>>($"/testing/messages/topics/{TopicName}");
        Assert.NotNull(getResp);
        
        // This proves we return the union (2) rather than distinct messages (1)
        Assert.Equal(2, getResp.Count);

        var sub1Count = getResp.Count(m => m.Subscription == $"{TopicName}/subscriptions/{Subscription1}");
        var sub2Count = getResp.Count(m => m.Subscription == $"{TopicName}/subscriptions/{Subscription2}");

        Assert.Equal(1, sub1Count);
        Assert.Equal(1, sub2Count);
    }

    [Fact]
    public async Task GetSubscriptionMessages_RespectsFilters()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        var sender = client.CreateSender(FilterAppPropTopic); 
        // FilterAppPropTopic has:
        // - FilterRegionSub (region='us-east')
        // - FilterCatchAllSub (no filter)

        // Msg A: region=us-east (Should match both)
        var msgA = new ServiceBusMessage("msg-a") { ApplicationProperties = { ["region"] = "us-east" } };
        // Msg B: region=eu-west (Should match only catch-all)
        var msgB = new ServiceBusMessage("msg-b") { ApplicationProperties = { ["region"] = "eu-west" } };

        await sender.SendMessageAsync(msgA);
        await sender.SendMessageAsync(msgB);

        // Verify Region Sub (Expect 1: msgA)
        var regionResp = await AdminHttpClient.GetFromJsonAsync<List<AdminMessageResponse>>(
            $"/testing/messages/topics/{FilterAppPropTopic}/subscriptions/{FilterRegionSub}");
        Assert.NotNull(regionResp);
        Assert.Single(regionResp);
        // Note: checking content inside "message_debug" is brittle, but we know count is 1.

        // Verify Catch All Sub (Expect 2: msgA + msgB)
        var catchAllResp = await AdminHttpClient.GetFromJsonAsync<List<AdminMessageResponse>>(
            $"/testing/messages/topics/{FilterAppPropTopic}/subscriptions/{FilterCatchAllSub}");
        Assert.NotNull(catchAllResp);
        Assert.Equal(2, catchAllResp.Count);
    }
}

// Data models for JSON deserialization

public class AdminMessageResponse
{
    [JsonPropertyName("message")]
    public Envelope Message { get; set; }
    
    [JsonPropertyName("dlq")]
    public bool Dlq { get; set; }
}

public class AdminTopicMessageResponse
{
    [JsonPropertyName("subscription")]
    public string Subscription { get; set; }

    [JsonPropertyName("message")]
    public Envelope Message { get; set; }

    [JsonPropertyName("dlq")]
    public bool Dlq { get; set; }
}

public class Envelope
{
    [JsonPropertyName("sequence_number")]
    public ulong SequenceNumber { get; set; }

    [JsonPropertyName("delivery_count")]
    public uint DeliveryCount { get; set; }

    [JsonPropertyName("state")]
    public object State { get; set; } // Can be string "Available" or object

    [JsonPropertyName("message_debug")]
    public string MessageDebug { get; set; }
}
