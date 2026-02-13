using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Xunit;

namespace AzureServiceBusEmulator.IntegrationTests
{
    public class EmulatorTests
    {
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

            // Create client
            await using var client = new ServiceBusClient(connectionString, options);

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
            ServiceBusMessage message = new ServiceBusMessage(messageBody);

            // Send message
            await sender.SendMessageAsync(message);

            // Await result
            var received = await receiveTask;

            Assert.NotNull(received);
            Assert.Equal(messageBody, received.Body.ToString());
        }
    }
}
