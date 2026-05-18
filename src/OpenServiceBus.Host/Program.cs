using System.Reflection;
using OpenServiceBus.Amqp.DependencyInjection;
using OpenServiceBus.Amqp.Hosting;
using OpenServiceBus.Amqp.Lifecycle;
using OpenServiceBus.Amqp.Queues;
using OpenServiceBus.Amqp.Routing;
using OpenServiceBus.Host;
using OpenServiceBus.InMemoryStorage.DependencyInjection;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;
using OpenServiceBus.Management.Endpoints;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHealthChecks();
builder.Services
    .AddOptions<AmqpListenerOptions>()
    .Bind(builder.Configuration.GetSection("OpenServiceBus:Amqp"));
builder.Services.AddOpenServiceBusInMemoryStorage();
builder.Services.AddOpenServiceBusAmqp();
builder.Services.AddHostedService<ConfigBootstrapHostedService>();

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
