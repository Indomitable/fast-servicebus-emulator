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
    public static FastServiceBusEmulatorResourceBuilder AddFastServiceBusEmulator(this IDistributedApplicationBuilder builder, [ResourceName] string name)
    {
        var resource = new FastServiceBusEmulatorResource(name);

        var resourceBuilder = builder.AddResource(resource)
            .WithImage(DefaultContainerImage)
            .WithImageTag(DefaultContainerTag)
            .WithEndpoint(
                targetPort: FastServiceBusEmulatorResource.AmqpPort,
                name: FastServiceBusEmulatorResource.AmqpEndpointName,
                scheme: "tcp");

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
        var indent = new IndentScope();
        configBuilder.AppendLine("topology:");
        using (var queuesIndent = indent.Start())
        {
            WriteCollection(queuesIndent, "queues", topology.Queues, configBuilder);
            using (var queueIndent = queuesIndent.Start())
            {
                foreach (var queue in topology.Queues)
                {
                    WriteEntityConfig(queueIndent, queue, configBuilder);
                }
            }
        }

        using (var topicsIndent = indent.Start())
        {
            WriteCollection(topicsIndent, "topics", topology.Topics, configBuilder);
            using (var topicIndent = topicsIndent.Start())
            {
                foreach (var topic in topology.Topics)
                {
                    configBuilder.AppendLine($"{topicIndent}- name: \"{EscapeYamlString(topic.Name)}\"");
                    using (var subscriptionsIndent = topicIndent.Start())
                    {
                        WriteCollection(subscriptionsIndent, "subscriptions", topic.Subscriptions, configBuilder);
                        using (var subscriptionIdent = subscriptionsIndent.Start())
                        {
                            foreach (var sub in topic.Subscriptions)
                            {
                                WriteSubscription(subscriptionIdent, sub, configBuilder);
                            }
                        }
                    }
                }
            }
        }

        return configBuilder.ToString();
    }

    private static void WriteCollection<T>(IndentScope indent, string name, IReadOnlyList<T> collection, StringBuilder builder)
    {
        builder.Append($"{indent}{name}:");
        if (collection.Count == 0)
        {
            builder.Append(" []");
        }
        builder.AppendLine();
    }

    private static void WriteEntityConfig(IndentScope indent, BaseEntityConfig entityConfig, StringBuilder builder)
    {
        builder.AppendLine($"{indent}- name: \"{EscapeYamlString(entityConfig.Name)}\"");
        using var indentInner = indent.Start();
        if (entityConfig.LockDurationSeconds.HasValue)
            builder.AppendLine($"{indentInner}lock_duration_secs: {entityConfig.LockDurationSeconds.Value}");
        if (entityConfig.MaxDeliveryCount.HasValue)
            builder.AppendLine($"{indentInner}max_delivery_count: {entityConfig.MaxDeliveryCount.Value}");
        if (entityConfig.DefaultMessageTtlSeconds.HasValue)
            builder.AppendLine($"{indentInner}default_message_ttl_secs: {entityConfig.DefaultMessageTtlSeconds.Value}");
        if (entityConfig.DeadLetteringOnMessageExpiration.HasValue)
            builder.AppendLine($"{indentInner}dead_lettering_on_message_expiration: {entityConfig.DeadLetteringOnMessageExpiration.Value.ToString().ToLowerInvariant()}");
        if (entityConfig.MaxSize.HasValue)
            builder.AppendLine($"{indentInner}max_size: {entityConfig.MaxSize.Value}");
    }

    private static void WriteSubscription(IndentScope indent, Subscription sub, StringBuilder builder)
    {
        WriteEntityConfig(indent, sub, builder);
        if (sub.Filters.Count > 0)
        {
            using (var filtersIndent = indent.Start())
            {
                builder.AppendLine($"{filtersIndent}filters:");
                using (var filterIndent = filtersIndent.Start())
                {
                    foreach (var filter in sub.Filters)
                    {
                        if (filter is CorrelationFilter cf)
                        {
                            builder.AppendLine($"{filterIndent}- type: correlation");
                            using (var filterPropertyIndent = filterIndent.Start())
                            {
                                if (cf.CorrelationId is not null)
                                    builder.AppendLine(
                                        $"{filterPropertyIndent}correlation_id: \"{EscapeYamlString(cf.CorrelationId)}\"");
                                if (cf.MessageId is not null)
                                    builder.AppendLine(
                                        $"{filterPropertyIndent}message_id: \"{EscapeYamlString(cf.MessageId)}\"");
                                if (cf.To is not null)
                                    builder.AppendLine($"{filterPropertyIndent}to: \"{EscapeYamlString(cf.To)}\"");
                                if (cf.ReplyTo is not null)
                                    builder.AppendLine(
                                        $"{filterPropertyIndent}reply_to: \"{EscapeYamlString(cf.ReplyTo)}\"");
                                if (cf.Subject is not null)
                                    builder.AppendLine(
                                        $"{filterPropertyIndent}subject: \"{EscapeYamlString(cf.Subject)}\"");
                                if (cf.ContentType is not null)
                                    builder.AppendLine(
                                        $"{filterPropertyIndent}content_type: \"{EscapeYamlString(cf.ContentType)}\"");
                                if (cf.Properties is { Count: > 0 })
                                {
                                    builder.AppendLine($"{filterPropertyIndent}properties:");
                                    using (var propertyIndent = filterPropertyIndent.Start())
                                    {
                                        foreach (var prop in cf.Properties)
                                        {
                                            builder.AppendLine(
                                                $"{propertyIndent}\"{EscapeYamlString(prop.Key)}\": \"{EscapeYamlString(prop.Value)}\"");
                                        }
                                    }
                                }
                            }
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
}

public class IndentScope(int indent = 0): IDisposable
{
    private int indent = indent;

    private void Increase()
    {
        indent += 2;
    }

    private void Decrease()
    {
        indent -= 2;
    }
    
    public override string ToString() => new(' ', indent);

    public IndentScope Start()
    {
        var scope = new IndentScope(indent);
        scope.Increase();
        return scope;
    }
    
    public void Dispose()
    {
        Decrease();
    }
}

public sealed class Topology
{
    public IReadOnlyList<Queue> Queues { get; init; } = [];
    public IReadOnlyList<Topic> Topics { get; init; } = [];
}

public class BaseEntityConfig(string name)
{
    public string Name { get; } = name;
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

public sealed class Queue(string name) : BaseEntityConfig(name);

public sealed class Topic(string name)
{
    public string Name { get; } = name;
    public IReadOnlyList<Subscription> Subscriptions { get; init; } = [];
}

public sealed class Subscription(string name): BaseEntityConfig(name)
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