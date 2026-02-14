using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

/// <summary>
/// Tests for dead-letter queue functionality:
/// - Explicit dead-lettering via DeadLetterMessageAsync
/// - Automatic dead-lettering when max delivery count is exceeded
/// Each test uses a dedicated queue to avoid cross-test interference.
/// </summary>
public class DeadLetterTests : BaseServiceBusTest
{
    /// <summary>
    /// Dead-letter a message explicitly via DeadLetterMessageAsync.
    /// Then open a DLQ receiver and verify the message appears there.
    /// </summary>
    [Fact]
    public async Task PeekLock_DeadLetter_Message_Moves_To_DLQ()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(DlqExplicitQueue);

        // PeekLock receiver on main queue
        var receiver = client.CreateReceiver(DlqExplicitQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // DLQ receiver (ReceiveAndDelete for simplicity)
        var dlqReceiver = client.CreateReceiver(DlqExplicitQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            SubQueue = SubQueue.DeadLetter
        });

        // Send a message
        var messageBody = $"dlq-explicit-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

        // Receive in PeekLock
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(received);
        Assert.Equal(messageBody, received.Body.ToString());

        // Dead-letter it
        await receiver.DeadLetterMessageAsync(received, "TestReason", "Testing explicit dead-letter");

        // Main queue should be empty
        var mainNext = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(mainNext);

        // DLQ should have the message
        var dlqMessage = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(dlqMessage);
        Assert.Equal(messageBody, dlqMessage.Body.ToString());
    }

    /// <summary>
    /// Queue configured with max_delivery_count: 2.
    /// Send a message, receive+abandon it twice. On the 3rd receive attempt,
    /// the message should NOT be in the main queue (it was auto-dead-lettered).
    /// Then verify it's in the DLQ.
    /// </summary>
    [Fact]
    public async Task PeekLock_MaxDeliveryCount_Exceeded_Auto_DeadLetters()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(DlqAutoQueue);

        // PeekLock receiver on main queue
        var receiver = client.CreateReceiver(DlqAutoQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // DLQ receiver
        var dlqReceiver = client.CreateReceiver(DlqAutoQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            SubQueue = SubQueue.DeadLetter
        });

        // Send a message
        var messageBody = $"dlq-auto-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

        // First receive + abandon (delivery count → 1)
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(first);
        Assert.Equal(messageBody, first.Body.ToString());
        await receiver.AbandonMessageAsync(first);

        // Second receive + abandon (delivery count → 2, equals max_delivery_count)
        // On this abandon, the store should auto-dead-letter the message.
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(second);
        Assert.Equal(messageBody, second.Body.ToString());
        await receiver.AbandonMessageAsync(second);

        // Main queue should be empty now — message was auto-dead-lettered
        var mainNext = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(mainNext);

        // DLQ should have the message
        var dlqMessage = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(dlqMessage);
        Assert.Equal(messageBody, dlqMessage.Body.ToString());
    }
}
