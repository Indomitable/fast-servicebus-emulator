using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

public class SingleReceiverTests: BaseServiceBusTest
{
    [Fact]
    public async Task Send_To_Topic_TwoSubscribers_SameSubscription_OnlyOneReceives()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        // Use a dedicated topic/subscription to avoid contention with EmulatorTests
        const string competingTopic = "competing-topic";
        const string sharedSub = "shared-sub";

        await using var client = new ServiceBusClient(ConnectionString, options);
        // Create sender for the topic
        await using var sender = client.CreateSender(competingTopic);

        // Create two receivers for the SAME subscription (competing consumers)
        await using var receiver1 = client.CreateReceiver(competingTopic, sharedSub, new ServiceBusReceiverOptions 
        { 
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
        });
        await using var receiver2 = client.CreateReceiver(competingTopic, sharedSub, new ServiceBusReceiverOptions 
        { 
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
        });

        // Start both receive tasks
        var receiveTask1 = receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        var receiveTask2 = receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(3));

        // Send message to the topic
        var messageBody = $"Topic fanout test - {Guid.NewGuid()}";
        var message = new ServiceBusMessage(messageBody);
        await sender.SendMessageAsync(message);

        // Both subscriptions should receive the message
        var received1 = await receiveTask1;
        var received2 = await receiveTask2;

        if (received1 is not null && received2 is not null)
        {
            Assert.Fail("Both subscriptions received the message.");
        }
        if (received1 is null && received2 is null)
        {
            Assert.Fail("Neither subscriptions received the message.");
        }
    }
    
    [Fact]
    public async Task SendMessage_To_Queue_OnlyOneReceiverReceives()
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        // Create client
        await using var client = new ServiceBusClient(ConnectionString, options);

        // Use a separate queue to avoid competing with EmulatorTests
        const string competingQueue = "processing-queue";

        // Create sender
        await using var sender = client.CreateSender(competingQueue);

        await using var receiver1 = client.CreateReceiver(competingQueue, new ServiceBusReceiverOptions 
        { 
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
        });
        
        await using var receiver2 = client.CreateReceiver(competingQueue, new ServiceBusReceiverOptions 
        { 
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
        });

        // Start receiving task
        var receiveTask1 = receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        var receiveTask2 = receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(3));

        // Create message
        string messageBody = $"Hello from .NET 10 Integration Test - {Guid.NewGuid()}";
        var message = new ServiceBusMessage(messageBody);
            
        // Send message
        await sender.SendMessageAsync(message);

        // Await result
        var received1 = await receiveTask1;
        var received2 = await receiveTask2;

        if (received1 is not null && received2 is not null)
        {
            Assert.Fail("Both subscriptions received the message.");
        }

        if (received1 is null && received2 is null)
        {
            Assert.Fail("Neither subscriptions received the message.");
        }
    }
}