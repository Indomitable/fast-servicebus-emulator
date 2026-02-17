using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

/// <summary>
/// Tests for message time-to-live (TTL) functionality:
/// - Messages with short TTL expire and are discarded
/// - Messages with TTL on a dead_lettering_on_message_expiration queue go to DLQ
/// </summary>
public class MessageTtlTests : BaseServiceBusTest
{
    /// <summary>
    /// Send a message with a 1-second TTL to a queue with no dead-lettering on expiration.
    /// Wait for it to expire, then try to receive. Should return null (message discarded).
    /// </summary>
    [Fact]
    public async Task Message_With_Short_TTL_Expires_And_Is_Not_Received()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        await using var sender = client.CreateSender(TtlDiscardQueue);

        // Send a message with 1-second TTL
        var messageBody = $"ttl-discard-{Guid.NewGuid()}";
        var message = new ServiceBusMessage(messageBody)
        {
            TimeToLive = TimeSpan.FromSeconds(1)
        };
        await sender.SendMessageAsync(message);

        // Wait for expiry
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Try to receive — should return null (expired and discarded)
        await using var receiver = client.CreateReceiver(TtlDiscardQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(received);
    }

    /// <summary>
    /// Queue configured with default_message_ttl_secs: 2 and dead_lettering_on_message_expiration: true.
    /// Send a message (no per-message TTL — inherits entity default), wait for expiry,
    /// then verify it's in the DLQ.
    /// </summary>
    [Fact]
    public async Task Message_With_EntityDefault_TTL_Dead_Lettered_On_Expiration()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        await using var sender = client.CreateSender(TtlDlqQueue);

        // Send a message (no per-message TTL — uses entity default of 2 seconds)
        var messageBody = $"ttl-dlq-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

        // Wait for expiry (entity default is 2s, wait 4s to be safe)
        await Task.Delay(TimeSpan.FromSeconds(4));

        // Main queue should be empty — message expired
        await using var receiver = client.CreateReceiver(TtlDlqQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var mainMsg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(mainMsg);

        // DLQ should have the message
        await using var dlqReceiver = client.CreateReceiver(TtlDlqQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            SubQueue = SubQueue.DeadLetter
        });
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(dlqMsg);
        Assert.Equal(messageBody, dlqMsg.Body.ToString());
    }
}
