using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

/// <summary>
/// Tests for subscription correlation filter routing:
/// - Filter by Subject (system property)
/// - Filter by custom application property
/// 
/// Topology:
///   filter-topic:
///     - orders-sub: correlation filter on subject = "order-created"
///     - all-sub: no filter (catch-all)
///   filter-appprop-topic:
///     - region-sub: correlation filter on properties.region = "us-east"
///     - catch-all-sub: no filter (catch-all)
/// </summary>
public class CorrelationFilterTests : BaseServiceBusTest
{
    /// <summary>
    /// Send two messages to filter-topic: one with Subject="order-created",
    /// one with Subject="other-event". The orders-sub (filtered on subject)
    /// should only get the matching message. The all-sub (no filter) gets both.
    /// </summary>
    [Fact]
    public async Task Correlation_Filter_Routes_By_Subject()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(FilterTopic);

        // Create receivers
        var ordersReceiver = client.CreateReceiver(FilterTopic, FilterOrdersSub, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var allReceiver = client.CreateReceiver(FilterTopic, FilterAllSub, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });

        // Send a matching message (Subject = "order-created")
        var matchBody = $"order-match-{Guid.NewGuid()}";
        var matchMsg = new ServiceBusMessage(matchBody) { Subject = "order-created" };
        await sender.SendMessageAsync(matchMsg);

        // Send a non-matching message (Subject = "other-event")
        var otherBody = $"other-event-{Guid.NewGuid()}";
        var otherMsg = new ServiceBusMessage(otherBody) { Subject = "other-event" };
        await sender.SendMessageAsync(otherMsg);

        // all-sub should receive both messages
        var allMsg1 = await allReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(allMsg1);
        var allMsg2 = await allReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(allMsg2);
        var allBodies = new[] { allMsg1.Body.ToString(), allMsg2.Body.ToString() };
        Assert.Contains(matchBody, allBodies);
        Assert.Contains(otherBody, allBodies);

        // orders-sub should only receive the matching message
        var ordersMsg = await ordersReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(ordersMsg);
        Assert.Equal(matchBody, ordersMsg.Body.ToString());

        // orders-sub should have no more messages
        var ordersNext = await ordersReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(ordersNext);
    }

    /// <summary>
    /// Send messages with different application properties to filter-appprop-topic.
    /// The region-sub (filtered on properties.region = "us-east") should only get
    /// messages with that property. The catch-all-sub gets all messages.
    /// </summary>
    [Fact]
    public async Task Correlation_Filter_Routes_By_ApplicationProperty()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(FilterAppPropTopic);

        // Create receivers
        var regionReceiver = client.CreateReceiver(FilterAppPropTopic, FilterRegionSub, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var catchAllReceiver = client.CreateReceiver(FilterAppPropTopic, FilterCatchAllSub, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });

        // Send a matching message (region = "us-east")
        var matchBody = $"region-match-{Guid.NewGuid()}";
        var matchMsg = new ServiceBusMessage(matchBody);
        matchMsg.ApplicationProperties["region"] = "us-east";
        await sender.SendMessageAsync(matchMsg);

        // Send a non-matching message (region = "eu-west")
        var otherBody = $"region-other-{Guid.NewGuid()}";
        var otherMsg = new ServiceBusMessage(otherBody);
        otherMsg.ApplicationProperties["region"] = "eu-west";
        await sender.SendMessageAsync(otherMsg);

        // catch-all-sub should receive both messages
        var all1 = await catchAllReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(all1);
        var all2 = await catchAllReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(all2);
        var allBodies = new[] { all1.Body.ToString(), all2.Body.ToString() };
        Assert.Contains(matchBody, allBodies);
        Assert.Contains(otherBody, allBodies);

        // region-sub should only receive the matching message
        var regionMsg = await regionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(regionMsg);
        Assert.Equal(matchBody, regionMsg.Body.ToString());

        // region-sub should have no more messages
        var regionNext = await regionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(regionNext);
    }
}
