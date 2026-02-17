using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;

namespace AzureServiceBusEmulator.IntegrationTests;

public class ServiceBusProcessorTests: BaseServiceBusTest
{
    [Fact]
    public async Task ProcessorReceiveMessage()
    {
        await using var client = new ServiceBusClient(ConnectionString);

        await using var sender = client.CreateSender(QueueName);

        await using var processor = client.CreateProcessor(QueueName, new ServiceBusProcessorOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            MaxConcurrentCalls = 2,
        });

        ConcurrentBag<string> messages = new ConcurrentBag<string>();

        processor.ProcessMessageAsync += ReceiveMessage;
        processor.ProcessErrorAsync += ReceiveError;
        await processor.StartProcessingAsync();

        var messageBody = "Message 1";
        var message = new ServiceBusMessage(messageBody);
        await sender.SendMessageAsync(message);
        
        messageBody = "Message 2";
        message = new ServiceBusMessage(messageBody);
        await sender.SendMessageAsync(message);

        await WaitUntil(() => messages.Count == 2, TimeSpan.FromSeconds(10));
        
        Assert.Equal(2, messages.Count);

        await processor.StopProcessingAsync();
        
        processor.ProcessMessageAsync -= ReceiveMessage;
        processor.ProcessErrorAsync -= ReceiveError;
        
        return;

        async Task ReceiveMessage(ProcessMessageEventArgs args)
        {
            messages.Add(args.Message.Body.ToString());
            await args.CompleteMessageAsync(args.Message);
        }
        
        Task ReceiveError(ProcessErrorEventArgs arg)
        {
            Assert.Fail("Error received");
            return Task.CompletedTask;
        }
    }
}