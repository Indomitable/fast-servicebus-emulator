using FastServiceBusEmulator.Aspire.Hosting;
using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var serviceBus = builder.AddFastServiceBusEmulator("servicebus")
    .WithTopology(new Topology
    {
        Queues = [
            new Queue("input-queue")
            {
                DeadLetteringOnMessageExpiration = true,
                LockDurationSeconds = 10,
                MaxDeliveryCount = 3,
                MaxSize = 100,
                DefaultMessageTtlSeconds = 1000
            },
            new Queue("processing-queue")
        ],
        Topics = [
            new Topic("topic \"test\"")
            {
                Subscriptions = [
                    new Subscription("sub-1")
                    {
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
                                MessageId =  "message\\1",
                                To =  "sub-2",
                                ReplyTo =  "sub-3",
                                CorrelationId =  "sub-4",
                                Properties = new Dictionary<string, string> {
                                    ["prop1"] = "value1"
                                }
                            }
                        ]
                    },
                    new Subscription("sub-2")
                ]
            },
            new Topic("events")
            {
                Subscriptions = [
                    new Subscription("sub-1")
                ]
            }
        ]
    });

builder.AddProject<FastServiceBusEmulatorTestProject>("test-project")
    .WithReference(serviceBus);

builder.Build().Run();
