using Azure.Messaging.ServiceBus;

namespace FastServiceBusEmulator.IntegrationTests;

/// <summary>
/// Tests for backpressure when a queue reaches its max_size capacity.
/// The backpressure-queue is configured with max_size: 10.
/// When full, the emulator rejects messages with amqp:resource-limit-exceeded.
/// </summary>
public class BackpressureTests : BaseServiceBusTest
{
    /// <summary>
    /// Fill the backpressure-queue (max_size=10) to capacity, then verify
    /// that sending another message is rejected by the emulator.
    /// The Azure SDK should throw a ServiceBusException on rejection.
    /// </summary>
    [Fact]
    public async Task Send_To_Full_Queue_Is_Rejected()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        await using var sender = client.CreateSender(BackpressureQueue);

        // Drain any leftover messages from previous tests
        var drainReceiver = client.CreateReceiver(BackpressureQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        while (true)
        {
            var leftover = await drainReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
            if (leftover == null) break;
        }
        // Close the drain receiver so its AMQP link is detached and no more
        // flow credits are outstanding.  Without this, the receiver keeps
        // consuming messages as fast as they arrive and the store never fills.
        await drainReceiver.DisposeAsync();

        // Fill the queue to capacity (10 messages)
        for (int i = 0; i < 10; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"bp-{i}-{Guid.NewGuid()}"), TestContext.Current.CancellationToken);
        }

        // The 11th message should be rejected
        var ex = await Assert.ThrowsAsync<ServiceBusException>(async () =>
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"bp-overflow-{Guid.NewGuid()}"), TestContext.Current.CancellationToken);
        });

        // The exception should indicate the queue is full
        Assert.Contains("full", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Fill the queue, drain some messages, then verify sending works again.
    /// This proves backpressure is dynamic — once space is freed, messages are accepted.
    /// </summary>
    [Fact]
    public async Task Send_Succeeds_After_Draining_Full_Queue()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using var client = new ServiceBusClient(ConnectionString, options);
        var sender = client.CreateSender(BackpressureQueue);

        // Drain any leftover messages from previous test
        var drainReceiver = client.CreateReceiver(BackpressureQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        while (true)
        {
            var leftover = await drainReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
            if (leftover == null) break;
        }
        // Close the drain receiver before filling the queue
        await drainReceiver.DisposeAsync();

        // Fill to capacity
        for (int i = 0; i < 10; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"bp-drain-{i}-{Guid.NewGuid()}"), TestContext.Current.CancellationToken);
        }

        // Drain 5 messages to free space
        var receiver = client.CreateReceiver(BackpressureQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        for (int i = 0; i < 5; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.NotNull(msg);
        }
        // Close the receiver to free credits before sending more
        await receiver.DisposeAsync();

        // Should now be able to send again
        var newBody = $"bp-after-drain-{Guid.NewGuid()}";
        await sender.SendMessageAsync(new ServiceBusMessage(newBody), TestContext.Current.CancellationToken);

        // Verify we can receive the new message
        var verifyReceiver = client.CreateReceiver(BackpressureQueue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var received = await verifyReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.NotNull(received);
    }
}
