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
        if (topology.Queues.Any(q => string.IsNullOrEmpty(q.Name) || topology.Topics.Any(t =>
                string.IsNullOrEmpty(t.Name) || t.Subscriptions.Any(s => string.IsNullOrEmpty(s.Name)))))
        {
            throw new ArgumentException("Queue, Topic and Subscription names cannot be empty.", nameof(topology));
        }
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
        var indent0 = CreateIndent(2);
        configBuilder.Append($"{indent0}queues:");
        if (topology.Queues.Count == 0)
        {
            configBuilder.Append(" []");
        }
        configBuilder.AppendLine();
        foreach (var queue in topology.Queues)
        {
            WriteEntityConfig(queue, configBuilder);
        }
        configBuilder.Append($"{indent0}topics:");
        if (topology.Topics.Count == 0)
        {
            configBuilder.Append(" []");
        }
        configBuilder.AppendLine();
        foreach (var topic in topology.Topics)
        {
            var indent1 = CreateIndent(4);
            configBuilder.AppendLine($"{indent1}- name: \"{EscapeYamlString(topic.Name)}\"");
            var indent2 = CreateIndent(6);
            configBuilder.Append($"{indent2}subscriptions:");
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

    private static void WriteEntityConfig(BaseEntityConfig entityConfig, StringBuilder builder, int indent = 4)
    {
        var indent0 = CreateIndent(indent);
        var indent1 = CreateIndent(indent + 2);
        builder.AppendLine($"{indent0}- name: \"{EscapeYamlString(entityConfig.Name)}\"");
        if (entityConfig.LockDurationSeconds.HasValue)
            builder.AppendLine($"{indent1}lock_duration_secs: {entityConfig.LockDurationSeconds.Value}");
        if (entityConfig.MaxDeliveryCount.HasValue)
            builder.AppendLine($"{indent1}max_delivery_count: {entityConfig.MaxDeliveryCount.Value}");
        if (entityConfig.DefaultMessageTtlSeconds.HasValue)
            builder.AppendLine($"{indent1}default_message_ttl_secs: {entityConfig.DefaultMessageTtlSeconds.Value}");
        if (entityConfig.DeadLetteringOnMessageExpiration.HasValue)
            builder.AppendLine($"{indent1}dead_lettering_on_message_expiration: {entityConfig.DeadLetteringOnMessageExpiration.Value.ToString().ToLowerInvariant()}");
        if (entityConfig.MaxSize.HasValue)
            builder.AppendLine($"{indent1}max_size: {entityConfig.MaxSize.Value}");
    }

    private static void WriteSubscription(Subscription sub, StringBuilder builder)
    {
        var indent = 8;
        WriteEntityConfig(sub, builder, indent);
        if (sub.Filters.Count > 0)
        {
            var filtersIndent = CreateIndent(indent + 2);
            builder.AppendLine($"{filtersIndent}filters:");
            var filterIndent = CreateIndent(indent + 4);
            var filterPropertyIndent = CreateIndent(indent + 6);
            foreach (var filter in sub.Filters)
            {
                if (filter is CorrelationFilter cf)
                {
                    builder.AppendLine($"{filterIndent}- type: correlation");
                    if (cf.CorrelationId is not null)
                        builder.AppendLine($"{filterPropertyIndent}correlation_id: \"{EscapeYamlString(cf.CorrelationId)}\"");
                    if (cf.MessageId is not null)
                        builder.AppendLine($"{filterPropertyIndent}message_id: \"{EscapeYamlString(cf.MessageId)}\"");
                    if (cf.To is not null)
                        builder.AppendLine($"{filterPropertyIndent}to: \"{EscapeYamlString(cf.To)}\"");
                    if (cf.ReplyTo is not null)
                        builder.AppendLine($"{filterPropertyIndent}reply_to: \"{EscapeYamlString(cf.ReplyTo)}\"");
                    if (cf.Subject is not null)
                        builder.AppendLine($"{filterPropertyIndent}subject: \"{EscapeYamlString(cf.Subject)}\"");
                    if (cf.ContentType is not null)
                        builder.AppendLine($"{filterPropertyIndent}content_type: \"{EscapeYamlString(cf.ContentType)}\"");
                    if (cf.Properties is { Count: > 0 })
                    {
                        var propertyIndent = CreateIndent(indent + 8);
                        builder.AppendLine($"{filterPropertyIndent}properties:");
                        foreach (var prop in cf.Properties)
                        {
                            builder.AppendLine($"{propertyIndent}\"{EscapeYamlString(prop.Key)}\": \"{EscapeYamlString(prop.Value)}\"");
                        }
                    }
                }
            }
        }
    }
    
    private static string EscapeYamlString(string value)
    {
        return value.Replace("\\", "\\\\").Replace("\"", "\\\"");
    }
    
    private static string CreateIndent(int count) => new(' ', count);
}

public sealed class Topology
{
    public IReadOnlyList<Queue> Queues { get; init; } = [];
    public IReadOnlyList<Topic> Topics { get; init; } = [];
}

public class BaseEntityConfig
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

public sealed class Queue : BaseEntityConfig;

public sealed class Topic
{
    public string Name { get; init; } = string.Empty;
    public IReadOnlyList<Subscription> Subscriptions { get; init; } = [];
}

public sealed class Subscription: BaseEntityConfig
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