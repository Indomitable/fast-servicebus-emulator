using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

var builder = DistributedApplication.CreateBuilder(args);

builder.Services.AddHealthChecks()
    .AddCheck<ServiceBuseEmulatorHealthCheck>("servicebus");

var projectPath = Assembly.GetExecutingAssembly().GetCustomAttributes<AssemblyMetadataAttribute>().Single(m => m.Key == "ProjectLocation");;

var path = Path.GetFullPath(Path.Combine(projectPath.Value, "../.."));

builder.AddExecutable("servicebus", "cargo", path)
    .WithArgs("run")
    .WithHealthCheck("servicebus");

builder.Build().Run();

class ServiceBuseEmulatorHealthCheck : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = new CancellationToken())
    {
        using var client = new HttpClient();
        client.BaseAddress = new Uri("http://localhost:45672");
        using var response = await client.GetAsync("/testing/messages", cancellationToken);
        return response.IsSuccessStatusCode ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy();
    }
}
