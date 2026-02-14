namespace AzureServiceBusEmulator.IntegrationTests;

public class BaseServiceBusTest
{
    protected const string ConnectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";
    protected const string QueueName = "input-queue";
    protected const string TopicName = "events-topic";

    protected const string Subscription1 = "sub-1";
    protected const string Subscription2 = "sub-2";
}