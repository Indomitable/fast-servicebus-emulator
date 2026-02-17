using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

/// <summary>
/// Tests that the emulator correctly respects AMQP link credit flow control,
/// which is driven by the client's PrefetchCount setting.
///
/// PrefetchCount = 0 (default): SDK grants credit on-demand (1 at a time per ReceiveMessageAsync call)
/// PrefetchCount = N: SDK grants N credits up front, replenishes as consumed
///
/// The fe2o3-amqp sender naturally blocks when credit is 0, so these tests
/// verify the end-to-end flow works without deadlocks or message loss.
/// </summary>
public class LinkCreditTests : BaseServiceBusTest
{
    private const string PrefetchQueue = "prefetch-queue";

    /// <summary>
    /// With PrefetchCount = 0 (default on-demand credit), send 5 messages
    /// and receive them one at a time. Each ReceiveMessageAsync call grants
    /// exactly 1 credit, so the server sends exactly 1 message per call.
    /// </summary>
    [Fact]
    public async Task Receive_WithDefaultPrefetch_DeliversMessagesOnDemand()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        await using var sender = client.CreateSender(PrefetchQueue);

        // Send 5 messages
        const int messageCount = 5;
        var sentBodies = new List<string>();
        for (int i = 0; i < messageCount; i++)
        {
            var body = $"on-demand-{i}-{Guid.NewGuid()}";
            sentBodies.Add(body);
            await sender.SendMessageAsync(new ServiceBusMessage(body));
        }

        // Create receiver with default PrefetchCount (0 = on-demand credit)
        await using var receiver = client.CreateReceiver(PrefetchQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            PrefetchCount = 0
        });

        // Receive messages one at a time
        var receivedBodies = new List<string>();
        for (int i = 0; i < messageCount; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(msg);
            receivedBodies.Add(msg.Body.ToString());
        }

        // All messages should have been received
        Assert.Equal(messageCount, receivedBodies.Count);

        // Each sent message should appear exactly once in received
        foreach (var body in sentBodies)
        {
            Assert.Contains(body, receivedBodies);
        }
    }

    /// <summary>
    /// With PrefetchCount = 5, send 10 messages and receive them all.
    /// The SDK grants 5 credits up front, the server sends 5 messages immediately,
    /// then as the client consumes them the SDK replenishes credit and the
    /// remaining 5 are delivered.
    /// </summary>
    [Fact]
    public async Task Receive_WithPrefetchCount5_DeliversAllMessages()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        await using var sender = client.CreateSender(PrefetchQueue);

        // Send 10 messages
        const int messageCount = 10;
        var sentBodies = new List<string>();
        for (int i = 0; i < messageCount; i++)
        {
            var body = $"prefetch5-{i}-{Guid.NewGuid()}";
            sentBodies.Add(body);
            await sender.SendMessageAsync(new ServiceBusMessage(body));
        }

        // Create receiver with PrefetchCount = 5
        // This tells the SDK to grant 5 credits up front
        await using var receiver = client.CreateReceiver(PrefetchQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            PrefetchCount = 5
        });

        // Receive all 10 messages
        var receivedBodies = new List<string>();
        for (int i = 0; i < messageCount; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(msg);
            receivedBodies.Add(msg.Body.ToString());
        }

        // All messages should have been received
        Assert.Equal(messageCount, receivedBodies.Count);

        // Each sent message should appear exactly once in received
        foreach (var body in sentBodies)
        {
            Assert.Contains(body, receivedBodies);
        }
    }

    /// <summary>
    /// With PrefetchCount = 1, send 5 messages and receive them all one by one.
    /// This is the most restrictive credit setting — only 1 message buffered at a time.
    /// Verifies the server correctly handles minimal credit without deadlocking.
    /// </summary>
    [Fact]
    public async Task Receive_WithPrefetchCount1_DeliversMessagesOneByOne()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        await using var sender = client.CreateSender(PrefetchQueue);

        // Send 5 messages
        const int messageCount = 5;
        var sentBodies = new List<string>();
        for (int i = 0; i < messageCount; i++)
        {
            var body = $"prefetch1-{i}-{Guid.NewGuid()}";
            sentBodies.Add(body);
            await sender.SendMessageAsync(new ServiceBusMessage(body));
        }

        // Create receiver with PrefetchCount = 1
        await using var receiver = client.CreateReceiver(PrefetchQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            PrefetchCount = 1
        });

        // Receive all messages
        var receivedBodies = new List<string>();
        for (int i = 0; i < messageCount; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(msg);
            receivedBodies.Add(msg.Body.ToString());
        }

        Assert.Equal(messageCount, receivedBodies.Count);
        foreach (var body in sentBodies)
        {
            Assert.Contains(body, receivedBodies);
        }
    }

    /// <summary>
    /// Verifies that after receiving all prefetched messages, a subsequent
    /// ReceiveMessageAsync with no more messages available returns null
    /// (timeout), proving the server isn't sending extra messages beyond credit.
    /// </summary>
    [Fact]
    public async Task Receive_WithPrefetch_NoExtraMessagesDelivered()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        await using var sender = client.CreateSender(PrefetchQueue);

        // Send exactly 3 messages
        const int messageCount = 3;
        for (int i = 0; i < messageCount; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"bounded-{i}-{Guid.NewGuid()}"));
        }

        await using var receiver = client.CreateReceiver(PrefetchQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            PrefetchCount = 10 // More credit than messages available
        });

        // Receive the 3 messages
        for (int i = 0; i < messageCount; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(msg);
        }

        // Next receive should timeout — no more messages
        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(extra);
    }
}
