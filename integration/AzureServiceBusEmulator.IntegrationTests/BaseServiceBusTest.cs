namespace AzureServiceBusEmulator.IntegrationTests;

[CollectionDefinition(nameof(ServiceBusCollection))]
public class ServiceBusCollection;

[Collection(nameof(ServiceBusCollection))]
public class BaseServiceBusTest //: IAsyncLifetime
{
    protected const string ConnectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";

    // Basic tests
    protected const string QueueName = "input-queue";
    protected const string TopicName = "events-topic";
    protected const string Subscription1 = "sub-1";
    protected const string Subscription2 = "sub-2";

    // PeekLock tests — each test gets its own queue
    protected const string PeekLockCompleteQueue = "peeklock-complete-queue";
    protected const string PeekLockAbandonQueue = "peeklock-abandon-queue";
    protected const string PeekLockDeliveryCountQueue = "peeklock-deliverycount-queue";
    protected const string PeekLockBrokerPropsQueue = "peeklock-brokerprops-queue";

    // Dead-letter tests
    protected const string DlqExplicitQueue = "dlq-explicit-queue";
    protected const string DlqAutoQueue = "dlq-auto-queue"; // max_delivery_count: 2

    // TTL tests
    protected const string TtlDiscardQueue = "ttl-discard-queue";
    protected const string TtlDlqQueue = "ttl-dlq-queue"; // default_message_ttl_secs: 2, dead_lettering_on_message_expiration: true

    // Backpressure tests
    protected const string BackpressureQueue = "backpressure-queue"; // max_size: 10

    // Correlation filter tests
    protected const string FilterTopic = "filter-topic";
    protected const string FilterOrdersSub = "orders-sub";   // filter: subject = "order-created"
    protected const string FilterAllSub = "all-sub";         // no filter (catch-all)

    protected const string FilterAppPropTopic = "filter-appprop-topic";
    protected const string FilterRegionSub = "region-sub";   // filter: properties.region = "us-east"
    protected const string FilterCatchAllSub = "catch-all-sub"; // no filter (catch-all)
    protected const string FilterMultiTopic = "multi-filter-topic";
    protected const string FilterRegionSubMulti = "region-multi-filters-sub"; // has multiple filters.


    protected async Task WaitUntil(Func<bool> condition, TimeSpan maxWaitTime)
    {
        var token = new CancellationTokenSource(maxWaitTime).Token;
        while (!token.IsCancellationRequested)
        {
            if (condition())
            {
                return;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }
    }

    protected readonly HttpClient AdminHttpClient;
    private const string AdminBaseUrl = "http://localhost:45672";

    protected BaseServiceBusTest()
    {
        AdminHttpClient = new HttpClient();
        AdminHttpClient.BaseAddress = new Uri(AdminBaseUrl);
        AdminHttpClient.DeleteAsync($"/testing/messages").GetAwaiter().GetResult();
    }
    
    // private async Task ClearAllMessages()
    // {
    //     await ;
    // }

    // public async Task InitializeAsync()
    // {
    //     await ClearAllMessages();
    // }
    //
    // public Task DisposeAsync()
    // {
    //     return Task.CompletedTask;
    // }
}
