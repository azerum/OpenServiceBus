using System.Reflection;
using OpenServiceBus.Amqp;
using OpenServiceBus.Broker;
using OpenServiceBus.Management;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHealthChecks();
builder.Services
    .AddOptions<AmqpListenerOptions>()
    .Bind(builder.Configuration.GetSection("OpenServiceBus:Amqp"));
builder.Services.AddOpenServiceBusBroker();
builder.Services.AddOpenServiceBusAmqp();

var app = builder.Build();

var version = Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.0.0";

app.MapGet("/", () => Results.Json(new
{
    name = "OpenServiceBus",
    version,
    status = "ok",
}));

app.MapHealthChecks("/health");

app.MapQueueEndpoints();

app.Run();

// Exposed for WebApplicationFactory-based tests.
public partial class Program;
