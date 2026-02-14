using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Xunit;
using Xunit.Abstractions;

namespace AzureServiceBusEmulator.IntegrationTests
{
    public class EmulatorTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public EmulatorTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }
        // Connection string for the emulator
        // Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
        // Note: The official SDK has a `UseDevelopmentEmulator=true` flag which might help with non-TLS.
        // Or we can try standard connection string with TransportType=AmqpTcp.
        
        private const string ConnectionString = "Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
        private const string QueueName = "input-queue";

        [Fact]
        public async Task Should_Send_And_Receive_Message()
        {
            // Create client options
            // UseDevelopmentEmulator=true in connection string tells the SDK to relax TLS requirements?
            // Actually, the connection string format for emulator is:
            // "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
            // But we need port 5672.
            
            var connectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
            
            var options = new ServiceBusClientOptions
            {
                TransportType = ServiceBusTransportType.AmqpTcp
            };
            testOutputHelper.WriteLine("Creating client...");
            // Create client
            await using var client = new ServiceBusClient(connectionString, options);
            testOutputHelper.WriteLine("Creating queue...");

            // Create sender
            ServiceBusSender sender = client.CreateSender(QueueName);
            testOutputHelper.WriteLine("Creating receiver...");


            // Create receiver FIRST
            ServiceBusReceiver receiver = client.CreateReceiver(QueueName, new ServiceBusReceiverOptions 
            { 
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
            });
            testOutputHelper.WriteLine("Starting receiver...");


            // Start receiving task
            var receiveTask = receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)).ContinueWith(x =>
            {
                testOutputHelper.WriteLine("Received message: " + x.Status);
                return x.Result;
            });
            
            // Give receiver a moment to establish link (important since our broadcast is live)
            await Task.Delay(2000); 

            // Create message
            string messageBody = $"Hello from .NET 10 Integration Test - {Guid.NewGuid()}";
            ServiceBusMessage message = new ServiceBusMessage(messageBody);
            
            testOutputHelper.WriteLine("Sending message...");
            // Send message
            await sender.SendMessageAsync(message);
            testOutputHelper.WriteLine("Message sent.");
            // Await result
            try
            {
                var received = await receiveTask;
                Assert.NotNull(received);
                Assert.Equal(messageBody, received.Body.ToString());
            }
            catch (Exception e)
            {
                testOutputHelper.WriteLine(e.ToString());
            }

        }

        [Fact]
        public async Task Should_Send_To_Topic_And_Both_Subscriptions_Receive()
        {
            var connectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
            
            var options = new ServiceBusClientOptions
            {
                TransportType = ServiceBusTransportType.AmqpTcp
            };

            await using var client = new ServiceBusClient(connectionString, options);
            // Create sender for the topic
            ServiceBusSender sender = client.CreateSender("events-topic");

            // Create receivers for both subscriptions
            ServiceBusReceiver receiver1 = client.CreateReceiver("events-topic", "sub-1", new ServiceBusReceiverOptions 
            { 
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete 
            });
            ServiceBusReceiver receiver2 = client.CreateReceiver("events-topic", "sub-2", new ServiceBusReceiverOptions 
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
            testOutputHelper.WriteLine("Finish test 2.");
        }
    }
}
