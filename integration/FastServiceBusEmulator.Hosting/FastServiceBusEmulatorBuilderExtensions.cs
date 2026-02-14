using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace FastServiceBusEmulator.Hosting;

/// <summary>
/// Extension methods for adding the Fast Service Bus Emulator to an Aspire application.
/// </summary>
public static class FastServiceBusEmulatorBuilderExtensions
{
    private const string DefaultContainerImage = "localhost/fast-servicebus-emulator";
    private const string DefaultContainerTag = "latest";

    /// <summary>
    /// Adds a Fast Service Bus Emulator container resource to the application.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The name of the resource.</param>
    /// <param name="configPath">Optional host path to config.yaml to bind-mount into the container.</param>
    /// <returns>A resource builder for the emulator.</returns>
    public static IResourceBuilder<FastServiceBusEmulatorResource> AddFastServiceBusEmulator(
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

        return resourceBuilder;
    }
}
