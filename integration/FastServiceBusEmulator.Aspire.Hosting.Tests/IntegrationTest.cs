namespace FastServiceBusEmulator.Aspire.Hosting.Tests;

public class SendReceiveTests(AspireFactory factory) : IntegrationTest(factory)
{
    [Fact]
    public async Task SendMessages_AndReceiveMessages()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var httpClient = Factory.GetTestProjectClient();

        var reader = new MessageReader(httpClient);
        Task.Run(() => reader.StartAsync(), TestContext.Current.CancellationToken);
        
        await httpClient.PostAsync("/message?message=1", null, cancellationToken);
        await httpClient.PostAsync("/message?message=2", null, cancellationToken);
        await httpClient.PostAsync("/message?message=3", null, cancellationToken);
        
        await Task.Delay(500, cancellationToken);
        await reader.StopAsync();
        var messages = reader.GetMessages();

        Assert.Equal(3, messages.Count);
    }

    [Fact]
    public async Task SendMessages_TestingApi_Receive()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var httpClient = Factory.GetTestProjectClient();
        var testingClient = Factory.GetServiceBusClient();
        
        var reader = new MessageReader(httpClient);
        Task.Run(() => reader.StartAsync(), TestContext.Current.CancellationToken);
        
        await testingClient.PostTopicMessageAsync("events", new StringMessageRequest
        {
            Content = "Test Message" 
        }, cancellationToken);
        

        await Task.Delay(500, cancellationToken);
        await reader.StopAsync();
        var messages = reader.GetMessages();

        var message = Assert.Single(messages);
        
        Assert.Equal("data: Test Message", message);
    }

    
}