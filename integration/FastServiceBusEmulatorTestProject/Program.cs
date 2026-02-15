using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<ServiceBusOptions>()
    .BindConfiguration("ServiceBus");
builder.Services.AddSingleton<ServiceBusClient>(sp =>
{
    var connectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("ServiceBus");
    return new ServiceBusClient(connectionString);
});

builder.Services.AddSingleton<ServiceBusSender>(sp =>
{
    var options = sp.GetRequiredService<IOptions<ServiceBusOptions>>().Value;
    var client = sp.GetRequiredService<ServiceBusClient>();
    return client.CreateSender(options.Topic.Name);
});

builder.Services.AddSingleton<Channel<string>>(_ => Channel.CreateUnbounded<string>());
builder.Services.AddHostedService<MessageReceiver>();

var app = builder.Build();

app.MapPost("/message", async ([FromQuery] string message, [FromServices] ServiceBusSender sender) =>
{
    await sender.SendMessageAsync(new ServiceBusMessage(message));
});

app.MapGet("/messages", ([FromServices] Channel<string> channel, CancellationToken token) => TypedResults.ServerSentEvents(GetMessages(channel, token)));

app.Run();

return;

async IAsyncEnumerable<string> GetMessages(Channel<string> channel, CancellationToken token)
{
    while (!token.IsCancellationRequested)
    {
        var message = await channel.Reader.ReadAsync(token);
        yield return message;
    }
}

public class ServiceBusOptions
{
    public ServiceBusTopicOptions Topic { get; set; } = new();
}

public class ServiceBusTopicOptions
{
    public string Name { get; set; } = string.Empty;
    public string Subscription { get; set; } = string.Empty;
}

public class MessageReceiver(ServiceBusClient client, IOptions<ServiceBusOptions> options, Channel<string> channel) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var receiver = client.CreateReceiver(options.Value.Topic.Name, options.Value.Topic.Subscription, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var writer = channel.Writer;
        
        while (!stoppingToken.IsCancellationRequested)
        {
            var message = await receiver.ReceiveMessageAsync(cancellationToken: stoppingToken);
            if (message is null)
            {
                continue;
            }
            await writer.WriteAsync(message.Body.ToString(), stoppingToken);
        }
        writer.Complete();
    }
}

