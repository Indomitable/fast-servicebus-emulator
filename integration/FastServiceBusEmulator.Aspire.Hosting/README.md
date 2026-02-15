# Fast Azure Service Bus Emulator

A high-performance, lightweight Azure Service Bus emulator for integration testing.
It implements the AMQP 1.0 protocol and mocks the Azure Service Bus behavior required by official SDKs.

## Usage 
```csharp
var builder = DistributedApplication.CreateBuilder(args);
builder.AddFastServiceBusEmulator("servicebus")
    .WithTopology(new Topology
    {
        Queues = [
            new Queue
            {
                Name = "input-queue",
                DeadLetteringOnMessageExpiration = true,
                LockDurationSeconds = 10,
                MaxDeliveryCount = 3,
                MaxSize = 100,
                DefaultMessageTtlSeconds = 1000
            }
        ],
        Topics = [
            new Topic
            {
                Name = "events-topic",
                Subscriptions = [
                    new Subscription
                    {
                        Name = "sub-1",
                        DeadLetteringOnMessageExpiration = true,
                        LockDurationSeconds = 10,
                        MaxDeliveryCount = 3,
                        MaxSize = 100,
                        DefaultMessageTtlSeconds = 1000,
                        Filters = [
                            new CorrelationFilter
                            {
                                ContentType = "application/json",
                                Subject = "sub-1",
                                MessageId =  "message-1",
                                To =  "sub-2",
                                ReplyTo =  "sub-3",
                                CorrelationId =  "sub-4",
                                Properties = new Dictionary<string, string> {
                                    ["prop1"] = "value1"
                                }
                            }
                        ]
                    },
                    new Subscription { Name = "sub-2" }
                ]
            }
        ]
    });
```