using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

/// <summary>
/// Tests for PeekLock receive mode: complete, abandon, delivery count, and broker properties.
/// Each test uses a dedicated queue to avoid cross-test interference.
/// </summary>
public class PeekLockTests : BaseServiceBusTest
{
    /// <summary>
    /// Receive a message in PeekLock mode and complete it.
    /// After completion, a subsequent receive should return null (message is gone).
    /// </summary>
    [Fact]
    public async Task PeekLock_Receive_And_Complete()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(PeekLockCompleteQueue);

        // Create PeekLock receiver
        var receiver = client.CreateReceiver(PeekLockCompleteQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // Send a message
        var messageBody = $"peeklock-complete-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

        // Receive in PeekLock — message is locked, not removed
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(received);
        Assert.Equal(messageBody, received.Body.ToString());

        // Complete the message — removes it from the queue
        await receiver.CompleteMessageAsync(received);

        // Subsequent receive should return null — message is gone
        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(next);
    }

    /// <summary>
    /// Receive a message in PeekLock mode and abandon it.
    /// After abandoning, the message should reappear on the next receive.
    /// </summary>
    [Fact]
    public async Task PeekLock_Receive_And_Abandon()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(PeekLockAbandonQueue);

        var receiver = client.CreateReceiver(PeekLockAbandonQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // Send a message
        var messageBody = $"peeklock-abandon-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

        // Receive and abandon
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(received);
        Assert.Equal(messageBody, received.Body.ToString());
        await receiver.AbandonMessageAsync(received);

        // Message should reappear
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(redelivered);
        Assert.Equal(messageBody, redelivered.Body.ToString());

        // Clean up — complete it so it doesn't linger
        await receiver.CompleteMessageAsync(redelivered);
    }

    /// <summary>
    /// Verify that DeliveryCount increments when a message is abandoned and redelivered.
    /// First delivery: DeliveryCount = 1. After abandon + redeliver: DeliveryCount = 2.
    /// </summary>
    [Fact]
    public async Task PeekLock_Abandoned_Message_Has_Incremented_DeliveryCount()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(PeekLockDeliveryCountQueue);

        var receiver = client.CreateReceiver(PeekLockDeliveryCountQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // Send a message
        var messageBody = $"peeklock-deliverycount-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

        // First receive — DeliveryCount should be 1
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(first);
        Assert.Equal(1, first.DeliveryCount);

        // Abandon
        await receiver.AbandonMessageAsync(first);

        // Second receive — DeliveryCount should be 2
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(second);
        Assert.Equal(messageBody, second.Body.ToString());
        Assert.Equal(2, second.DeliveryCount);

        // Clean up
        await receiver.CompleteMessageAsync(second);
    }

    /// <summary>
    /// Verify that broker-stamped properties (SequenceNumber, EnqueuedTime) are present
    /// on messages received in PeekLock mode.
    /// </summary>
    [Fact]
    public async Task PeekLock_Receive_Has_BrokerProperties()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(PeekLockBrokerPropsQueue);

        var receiver = client.CreateReceiver(PeekLockBrokerPropsQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // Send a message
        var messageBody = $"peeklock-brokerprops-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

        // Receive
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(received);

        // SequenceNumber should be > 0
        Assert.True(received.SequenceNumber > 0, $"SequenceNumber should be > 0, got {received.SequenceNumber}");

        // EnqueuedTime should be recent (within last 60 seconds)
        var elapsed = DateTimeOffset.UtcNow - received.EnqueuedTime;
        Assert.True(elapsed.TotalSeconds < 60, $"EnqueuedTime should be recent, but was {elapsed.TotalSeconds}s ago");
        Assert.True(elapsed.TotalSeconds >= 0, $"EnqueuedTime should not be in the future");

        // DeliveryCount should be >= 1
        Assert.True(received.DeliveryCount >= 1, $"DeliveryCount should be >= 1, got {received.DeliveryCount}");

        // Clean up
        await receiver.CompleteMessageAsync(received);
    }
}
