using Aspire.Hosting;
using Aspire.Hosting.Testing;
using FastServiceBusEmulator.Aspire.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Projects;

namespace FastServiceBusEmulator.IntegrationTests;

public class AspireFactory: IAsyncLifetime
{
    private DistributedApplication app = null!;
    
    public async ValueTask InitializeAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<FastServiceBusEmulator_IntegrationTests_AppHost>(TestContext.Current.CancellationToken);
        appHost.Services.ConfigureHttpClientDefaults(clientBuilder =>
        {
        });

        var ct = TestContext.Current.CancellationToken;
    
        app = await appHost.BuildAsync(ct).WaitAsync(ct);
        await app.StartAsync(ct);
        await app.ResourceNotifications.WaitForResourceHealthyAsync("servicebus", ct);
    }
    
    public async ValueTask DisposeAsync()
    {
        await app.StopAsync(TestContext.Current.CancellationToken);
        await app.DisposeAsync();
    }
    
    public TestingClient GetServiceBusClient() => new TestingClient(app.CreateHttpClient("servicebus"));
}
