using System.Text;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace FastServiceBusEmulator.Aspire.Hosting;

/// <summary>
/// Extension methods for adding the Fast Service Bus Emulator to an Aspire application.
/// </summary>
public static class FastServiceBusEmulatorBuilderExtensions
{
    private const string DefaultContainerImage = "indomitable/fast-servicebus-emulator";
    private const string DefaultContainerTag = "latest";

    /// <summary>
    /// Adds a Fast Service Bus Emulator container resource to the application.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The name of the resource.</param>
    /// <param name="configPath">Optional host path to config.yaml to bind-mount into the container.</param>
    /// <returns>A resource builder for the emulator.</returns>
    public static FastServiceBusEmulatorResourceBuilder AddFastServiceBusEmulator(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name,
        string? configPath = null)
    {
        var resource = new FastServiceBusEmulatorResource(name);

        var resourceBuilder = builder.AddResource(resource)
            .WithImage(DefaultContainerImage)
            .WithImageTag(DefaultContainerTag)
            .WithEndpoint(
                targetPort: FastServiceBusEmulatorResource.AmqpPort,
                name: FastServiceBusEmulatorResource.AmqpEndpointName,
                scheme: "tcp");

        if (configPath is not null)
        {
            resourceBuilder = resourceBuilder.WithBindMount(configPath, "/config/config.yaml");
        }

        return new FastServiceBusEmulatorResourceBuilder(resourceBuilder);
    }
}

public class FastServiceBusEmulatorResourceBuilder(IResourceBuilder<FastServiceBusEmulatorResource> builder) : IResourceBuilder<FastServiceBusEmulatorResource>
{
    public IResourceBuilder<FastServiceBusEmulatorResource> WithAnnotation<TAnnotation>(TAnnotation annotation,
        ResourceAnnotationMutationBehavior behavior = ResourceAnnotationMutationBehavior.Append) where TAnnotation : IResourceAnnotation =>
        builder.WithAnnotation(annotation, behavior);

    public IDistributedApplicationBuilder ApplicationBuilder => builder.ApplicationBuilder;
    public FastServiceBusEmulatorResource Resource => builder.Resource;

    public FastServiceBusEmulatorResourceBuilder WithTopology(Topology topology)
    {
        var config = BuildTopology(topology);
        this.WithContainerFiles("/config/", [
            new ContainerFile
            {
                Name =  "config.yaml",
                Contents = config,
                Mode = UnixFileMode.UserRead
            }
        ]);
        return this;
    }

    private static string BuildTopology(Topology topology)
    {
        var configBuilder = new StringBuilder();
        configBuilder.AppendLine("topology:");
        var ident0 = CreateIdent(2);
        configBuilder.Append($"{ident0}queues:");
        if (topology.Queues.Count == 0)
        {
            configBuilder.Append(" []");
        }
        configBuilder.AppendLine();
        foreach (var queue in topology.Queues)
        {
            WriteQueue(queue, configBuilder);
        }
        configBuilder.Append($"{ident0}topics:");
        if (topology.Topics.Count == 0)
        {
            configBuilder.Append(" []");
        }
        configBuilder.AppendLine();
        foreach (var topic in topology.Topics)
        {
            var ident1 = CreateIdent(4);
            configBuilder.AppendLine($"{ident1}- name: \"{topic.Name}\"");
            var ident2 = CreateIdent(6);
            configBuilder.Append($"{ident2}subscriptions:");
            if (topic.Subscriptions.Count == 0)
            {
                configBuilder.Append(" []");
            }
            configBuilder.AppendLine();
            foreach (var sub in topic.Subscriptions)
            {
                WriteSubscription(sub, configBuilder);
            }
        }
        return configBuilder.ToString();
    }

    private static void WriteQueue(Queue queue, StringBuilder builder, int ident = 4)
    {
        var ident0 = CreateIdent(ident);
        var ident1 = CreateIdent(ident + 2);
        builder.AppendLine($"{ident0}- name: \"{queue.Name}\"");
        if (queue.LockDurationSeconds.HasValue)
            builder.AppendLine($"{ident1}lock_duration_secs: {queue.LockDurationSeconds.Value}");
        if (queue.MaxDeliveryCount.HasValue)
            builder.AppendLine($"{ident1}max_delivery_count: {queue.MaxDeliveryCount.Value}");
        if (queue.DefaultMessageTtlSeconds.HasValue)
            builder.AppendLine($"{ident1}default_message_ttl_secs: {queue.DefaultMessageTtlSeconds.Value}");
        if (queue.DeadLetteringOnMessageExpiration.HasValue)
            builder.AppendLine($"{ident1}dead_lettering_on_message_expiration: {queue.DeadLetteringOnMessageExpiration.Value.ToString().ToLowerInvariant()}");
        if (queue.MaxSize.HasValue)
            builder.AppendLine($"{ident1}max_size: {queue.MaxSize.Value}");
    }

    private static void WriteSubscription(Subscription sub, StringBuilder builder)
    {
        var ident = 8;
        WriteQueue(sub, builder, ident);
        if (sub.Filters.Count > 0)
        {
            var filtersIdent = CreateIdent(ident + 2);
            builder.AppendLine($"{filtersIdent}filters:");
            var filterIdent = CreateIdent(ident + 4);
            var filterPropertyIdent = CreateIdent(ident + 6);
            foreach (var filter in sub.Filters)
            {
                if (filter is CorrelationFilter cf)
                {
                    builder.AppendLine($"{filterIdent}- type: correlation");
                    if (cf.CorrelationId is not null)
                        builder.AppendLine($"{filterPropertyIdent}correlation_id: \"{cf.CorrelationId}\"");
                    if (cf.MessageId is not null)
                        builder.AppendLine($"{filterPropertyIdent}message_id: \"{cf.MessageId}\"");
                    if (cf.To is not null)
                        builder.AppendLine($"{filterPropertyIdent}to: \"{cf.To}\"");
                    if (cf.ReplyTo is not null)
                        builder.AppendLine($"{filterPropertyIdent}reply_to: \"{cf.ReplyTo}\"");
                    if (cf.Subject is not null)
                        builder.AppendLine($"{filterPropertyIdent}subject: \"{cf.Subject}\"");
                    if (cf.ContentType is not null)
                        builder.AppendLine($"{filterPropertyIdent}content_type: \"{cf.ContentType}\"");
                    if (cf.Properties is { Count: > 0 })
                    {
                        var propertyIdent = CreateIdent(ident + 8);
                        builder.AppendLine($"{filterPropertyIdent}properties:");
                        foreach (var prop in cf.Properties)
                        {
                            builder.AppendLine($"{propertyIdent}{prop.Key}: \"{prop.Value}\"");
                        }
                    }
                }
            }
        }
    }
    
    private static string CreateIdent(int count) => string.Join("", Enumerable.Repeat(" ", count));
}

public sealed class Topology
{
    public IReadOnlyList<Queue> Queues { get; init; } = [];
    public IReadOnlyList<Topic> Topics { get; init; } = [];
}

public class Queue
{
    public string Name { get; init; } = string.Empty;
    /// <summary>
    /// Lock duration in seconds for PeekLock mode. Default: 30.
    /// </summary>
    public int? LockDurationSeconds { get; init; }
    /// <summary>
    /// Max delivery attempts before auto-dead-lettering. Default: 10.
    /// </summary>
    public int? MaxDeliveryCount { get; init; }
    /// <summary>
    /// Default message TTL in seconds. 0 = no expiry. Default: 0.
    /// </summary>
    public int? DefaultMessageTtlSeconds { get; init; }
    /// <summary>
    /// Whether to dead-letter expired messages. Default: false (discard).
    /// </summary>
    public bool? DeadLetteringOnMessageExpiration { get; init; }
    /// <summary>
    /// Maximum number of messages the store can hold. Default 1000.
    /// </summary>
    public int? MaxSize { get; init; }
}

public sealed class Topic
{
    public string Name { get; init; } = string.Empty;
    public IReadOnlyList<Subscription> Subscriptions { get; init; } = [];
}

public sealed class Subscription: Queue
{
   public IReadOnlyList<SubscriptionFilter> Filters { get; init; } = [];
}

public abstract class SubscriptionFilter;

public sealed class CorrelationFilter : SubscriptionFilter
{
    public string? CorrelationId { get; init; }
    public string? MessageId { get; init; }
    public string? To { get; init; }
    public string? ReplyTo { get; init; }
    public string? Subject { get; init; }
    public string? ContentType { get; init; }
    public Dictionary<string, string> Properties { get; init; } = [];
}