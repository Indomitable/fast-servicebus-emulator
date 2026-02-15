using Aspire.Hosting.ApplicationModel;

namespace FastServiceBusEmulator.Aspire.Hosting;

/// <summary>
/// Represents a Fast Service Bus Emulator container resource.
/// </summary>
/// <param name="name">The name of the resource.</param>
public class FastServiceBusEmulatorResource(string name)
    : ContainerResource(name), IResourceWithConnectionString
{
    internal const int AmqpPort = 5672;
    internal const string AmqpEndpointName = "amqp";

    /// <summary>
    /// Gets the AMQP endpoint reference for the emulator.
    /// </summary>
    public EndpointReference AmqpEndpoint => field ??= new EndpointReference(this, AmqpEndpointName);

    /// <summary>
    /// Gets the connection string expression for the Service Bus emulator.
    /// </summary>
    public ReferenceExpression ConnectionStringExpression =>
        ReferenceExpression.Create(
            $"Endpoint=sb://{AmqpEndpoint.Property(EndpointProperty.Host)}:{AmqpEndpoint.Property(EndpointProperty.Port)};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true");
}
