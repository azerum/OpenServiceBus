using System.Reflection;
using OpenServiceBus.Amqp.DependencyInjection;
using OpenServiceBus.Amqp.Hosting;
using OpenServiceBus.Amqp.Lifecycle;
using OpenServiceBus.Amqp.Queues;
using OpenServiceBus.Amqp.Routing;
using OpenServiceBus.Amqp.WebSockets;
using OpenServiceBus.Host;
using OpenServiceBus.InMemoryStorage.DependencyInjection;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;
using OpenServiceBus.Management.Endpoints;
using OpenServiceBus.SqliteStorage.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHealthChecks();
builder.Services
    .AddOptions<AmqpListenerOptions>()
    .Bind(builder.Configuration.GetSection("OpenServiceBus:Amqp"));
builder.Services
    .AddOptions<WebSocketBridgeOptions>()
    .Bind(builder.Configuration.GetSection("OpenServiceBus:WebSockets"));

// Storage mode selectable via OpenServiceBus:Storage:Mode (defaults to InMemory). When
// set to Sqlite the SQLite store registers IMessageStore first; the in-memory DI's TryAdd
// then becomes a no-op for the store while still wiring the queue/topic registries,
// transaction manager, lock manager, and message router that the rest of the broker needs.
var storageMode = builder.Configuration["OpenServiceBus:Storage:Mode"] ?? "InMemory";
if (string.Equals(storageMode, "Sqlite", StringComparison.OrdinalIgnoreCase))
{
    builder.Services.AddOpenServiceBusSqliteStorage(opt =>
    {
        var path = builder.Configuration["OpenServiceBus:Storage:DataSource"];
        if (!string.IsNullOrWhiteSpace(path)) opt.DataSource = path;
    });
}
builder.Services.AddOpenServiceBusInMemoryStorage();
builder.Services.AddOpenServiceBusAmqp();
builder.Services.AddHostedService<ConfigBootstrapHostedService>();
// When a persistent store is in use, recover the queue catalog from disk on startup
// so the registry agrees with the SQLite file after a restart.
builder.Services.AddHostedService<QueueRehydrationHostedService>();

var app = builder.Build();

var version = Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.0.0";

app.MapGet("/", () => Results.Json(new
{
    name = "OpenServiceBus",
    version,
    status = "ok",
    storage = storageMode,
}));

app.MapHealthChecks("/health");

app.MapQueueEndpoints();
app.MapTopicEndpoints();

app.Run();

// Exposed for WebApplicationFactory-based tests.
public partial class Program;
