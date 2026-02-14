using FastServiceBusEmulator.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

builder.AddFastServiceBusEmulator("servicebus");

builder.Build().Run();
