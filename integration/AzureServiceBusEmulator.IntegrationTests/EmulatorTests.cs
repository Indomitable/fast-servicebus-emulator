using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests
{
    public class EmulatorTests: BaseServiceBusTest
    {
        [Fact]
        public async Task Should_Send_And_Receive_Message()
        {
            var options = new ServiceBusClientOptions
            {
                TransportType = ServiceBusTransportType.AmqpTcp
            };

            // Create client
            await using var client = new ServiceBusClient(ConnectionString, options);

            // Create sender
            ServiceBusSender sender = client.CreateSender(QueueName);

            // Create receiver FIRST
            ServiceBusReceiver receiver = client.CreateReceiver(QueueName, new ServiceBusReceiverOptions 
            { 
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
            });

            // Start receiving task
            var receiveTask = receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            
            // Give receiver a moment to establish link (important since our broadcast is live)
            await Task.Delay(2000); 

            // Create message
            string messageBody = $"Hello from .NET 10 Integration Test - {Guid.NewGuid()}";
            var message = new ServiceBusMessage(messageBody);
            
            // Send message
            await sender.SendMessageAsync(message);

            // Await result
            var received = await receiveTask;
            Assert.NotNull(received);
            Assert.Equal(messageBody, received.Body.ToString());
        }

        [Fact]
        public async Task Should_Send_To_Topic_And_Both_Subscriptions_Receive()
        {
            var options = new ServiceBusClientOptions
            {
                TransportType = ServiceBusTransportType.AmqpTcp
            };

            await using var client = new ServiceBusClient(ConnectionString, options);
            // Create sender for the topic
            ServiceBusSender sender = client.CreateSender(TopicName);

            // Create receivers for both subscriptions
            ServiceBusReceiver receiver1 = client.CreateReceiver(TopicName, Subscription1, new ServiceBusReceiverOptions 
            { 
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
            });
            ServiceBusReceiver receiver2 = client.CreateReceiver(TopicName, Subscription2, new ServiceBusReceiverOptions 
            { 
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
            });

            // Start both receive tasks
            var receiveTask1 = receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            var receiveTask2 = receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

            // Give receivers time to establish links
            await Task.Delay(2000);

            // Send message to the topic
            string messageBody = $"Topic fanout test - {Guid.NewGuid()}";
            ServiceBusMessage message = new ServiceBusMessage(messageBody);
            await sender.SendMessageAsync(message);

            // Both subscriptions should receive the message
            var received1 = await receiveTask1;
            var received2 = await receiveTask2;

            Assert.NotNull(received1);
            Assert.NotNull(received2);
            Assert.Equal(messageBody, received1.Body.ToString());
            Assert.Equal(messageBody, received2.Body.ToString());
        }
        

    }
}
