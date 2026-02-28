using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using Aspire.Hosting;

namespace FastServiceBusEmulator.Aspire.Hosting.Tests;

public class AspireFactory: IAsyncLifetime
{
    private DistributedApplication app = null!;
    
    public async ValueTask InitializeAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.FastServiceBusEmulator_Aspire_Hosting_AppHost>(TestContext.Current.CancellationToken);

        appHost.Services.ConfigureHttpClientDefaults(clientBuilder =>
        {
            clientBuilder.AddStandardResilienceHandler();
        });

        var ct = TestContext.Current.CancellationToken;
    
        app = await appHost.BuildAsync(ct).WaitAsync(ct);
        await app.StartAsync(ct);
        await app.ResourceNotifications.WaitForResourceHealthyAsync("test-project", ct);
        await app.ResourceNotifications.WaitForResourceHealthyAsync("servicebus", ct);
    }
    
    public async ValueTask DisposeAsync()
    {
        await app.StopAsync(TestContext.Current.CancellationToken);
        await app.DisposeAsync();
    }
    
    public HttpClient GetTestProjectClient() => app.CreateHttpClient("test-project");
    
    public TestingClient GetServiceBusClient() => new TestingClient(app.CreateHttpClient("servicebus"));
}

[CollectionDefinition(DisableParallelization = true)]
public class AspireTestsCollection: ICollectionFixture<AspireFactory>;

[Collection<AspireTestsCollection>]
public class IntegrationTest
{
    protected AspireFactory Factory { get; }

    public IntegrationTest(AspireFactory factory)
    {
        Factory = factory;
    }

    protected class MessageReader(HttpClient httpClient)
    {
        private readonly List<string> messages = new List<string>();
        private CancellationTokenSource cts = new CancellationTokenSource();

        public async Task StartAsync()
        {
            await foreach (var message in ReadMessages(httpClient, cts.Token))
            {
                messages.Add(message);
            }
        }

        public async Task StopAsync()
        {
            await cts.CancelAsync();
        }
        
        public IReadOnlyList<string> GetMessages() => messages;
        
        private static async IAsyncEnumerable<string> ReadMessages(HttpClient httpClient, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, "/messages");
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));

            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var reader = new StreamReader(stream);

            StringBuilder messageBuilder = new();
            while (!cancellationToken.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync(cancellationToken);
                if (line is null)
                {
                    break;
                }

                if (string.IsNullOrEmpty(line))
                {
                    var message = messageBuilder.ToString().Trim();
                    messageBuilder.Clear();

                    if (!string.IsNullOrEmpty(message))
                    {
                        yield return message;
                    }
                }
                else
                {
                    messageBuilder.AppendLine(line);
                }
            }

            // Yield any remaining buffered content
            var remaining = messageBuilder.ToString().Trim();
            if (!string.IsNullOrEmpty(remaining))
            {
                yield return remaining;
            }
        }
    }
}