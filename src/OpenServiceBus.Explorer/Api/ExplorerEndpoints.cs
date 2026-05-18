using System.Net.Http.Json;
using System.Text;
using Azure.Messaging.ServiceBus;
using OpenServiceBus.Explorer.Sessions;

namespace OpenServiceBus.Explorer.Api;

public static class ExplorerEndpoints
{
    public static IEndpointRouteBuilder MapExplorerEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var api = endpoints.MapGroup("/api");

        // --- Queue CRUD (proxied to broker's management REST API) ---
        api.MapGet("/queues", async (string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.GetAsync(Combine(managementUrl, "/queues/"), ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapPut("/queues/{name}", async (string name, CreateQueueRequest body, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var payload = JsonContent.Create(body.Options);
            var resp = await http.PutAsync(Combine(body.ManagementUrl, $"/queues/{name}"), payload, ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapDelete("/queues/{name}", async (string name, string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.DeleteAsync(Combine(managementUrl, $"/queues/{name}"), ct);
            return Results.StatusCode((int)resp.StatusCode);
        });

        // --- Data plane (real Azure SDK against the broker) ---
        api.MapPost("/send", async (SendRequest req, SessionManager sessions, CancellationToken ct) =>
        {
            var session = sessions.GetOrCreate(req.ConnectionString);
            await using var sender = session.Sender(req.Queue);

            var msg = new ServiceBusMessage(req.Body ?? string.Empty);
            // Every message gets a MessageId — either the user-supplied one or an auto-generated
            // Guid — so the Explorer can always show a stable identifier in the received list.
            // The Service Bus SDK does NOT auto-generate this; without it the field is null on the wire.
            msg.MessageId = string.IsNullOrWhiteSpace(req.MessageId)
                ? Guid.NewGuid().ToString("N")
                : req.MessageId;
            if (!string.IsNullOrWhiteSpace(req.CorrelationId)) msg.CorrelationId = req.CorrelationId;
            if (!string.IsNullOrWhiteSpace(req.Subject)) msg.Subject = req.Subject;
            if (!string.IsNullOrWhiteSpace(req.ContentType)) msg.ContentType = req.ContentType;
            if (!string.IsNullOrWhiteSpace(req.ReplyTo)) msg.ReplyTo = req.ReplyTo;
            if (!string.IsNullOrWhiteSpace(req.To)) msg.To = req.To;
            if (!string.IsNullOrWhiteSpace(req.SessionId)) msg.SessionId = req.SessionId;
            if (!string.IsNullOrWhiteSpace(req.PartitionKey)) msg.PartitionKey = req.PartitionKey;
            if (req.TimeToLiveSeconds is > 0) msg.TimeToLive = TimeSpan.FromSeconds(req.TimeToLiveSeconds.Value);
            if (req.ScheduledEnqueueTime is { } scheduled) msg.ScheduledEnqueueTime = scheduled;
            if (req.Properties is { Count: > 0 })
            {
                foreach (var (k, v) in req.Properties) msg.ApplicationProperties[k] = v;
            }

            await sender.SendMessageAsync(msg, ct);
            return Results.Ok(new { sent = true, messageId = msg.MessageId, scheduledFor = msg.ScheduledEnqueueTime });
        });

        api.MapPost("/receive", async (ReceiveRequest req, SessionManager sessions, CancellationToken ct) =>
        {
            var session = sessions.GetOrCreate(req.ConnectionString);
            var receiver = session.Receiver(req.Queue);
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(req.TimeoutSeconds ?? 5), ct);
            if (msg is null)
            {
                return Results.Ok(new { received = false });
            }
            session.TrackLocked(msg);
            return Results.Ok(ToDto(msg));
        });

        api.MapPost("/complete", async (DispositionRequest req, SessionManager sessions, CancellationToken ct) =>
        {
            var session = sessions.GetOrCreate(req.ConnectionString);
            if (!session.TryTakeLocked(req.LockToken, out var msg) || msg is null)
            {
                return Results.NotFound(new { error = "Unknown lock token (already completed or never tracked by this explorer session)." });
            }
            await session.Receiver(req.Queue).CompleteMessageAsync(msg, ct);
            return Results.Ok(new { completed = true });
        });

        api.MapPost("/abandon", async (DispositionRequest req, SessionManager sessions, CancellationToken ct) =>
        {
            var session = sessions.GetOrCreate(req.ConnectionString);
            if (!session.TryTakeLocked(req.LockToken, out var msg) || msg is null)
            {
                return Results.NotFound(new { error = "Unknown lock token." });
            }
            await session.Receiver(req.Queue).AbandonMessageAsync(msg, cancellationToken: ct);
            return Results.Ok(new { abandoned = true });
        });

        api.MapPost("/deadletter", async (DeadLetterRequest req, SessionManager sessions, CancellationToken ct) =>
        {
            var session = sessions.GetOrCreate(req.ConnectionString);
            if (!session.TryTakeLocked(req.LockToken, out var msg) || msg is null)
            {
                return Results.NotFound(new { error = "Unknown lock token." });
            }
            await session.Receiver(req.Queue).DeadLetterMessageAsync(msg, req.Reason, req.Description, ct);
            return Results.Ok(new { deadLettered = true });
        });

        api.MapPost("/renew", async (DispositionRequest req, SessionManager sessions, CancellationToken ct) =>
        {
            var session = sessions.GetOrCreate(req.ConnectionString);
            // Renew uses the AMQP $management endpoint - does NOT consume the locked message,
            // so we look up but do NOT remove from the tracking dict.
            if (!session.TryPeekLocked(req.LockToken, out var msg) || msg is null)
            {
                return Results.NotFound(new { error = "Unknown lock token." });
            }
            await session.Receiver(req.Queue).RenewMessageLockAsync(msg, ct);
            return Results.Ok(new { renewedUntil = msg.LockedUntil });
        });

        api.MapPost("/defer", async (DispositionRequest req, SessionManager sessions, CancellationToken ct) =>
        {
            var session = sessions.GetOrCreate(req.ConnectionString);
            if (!session.TryTakeLocked(req.LockToken, out var msg) || msg is null)
            {
                return Results.NotFound(new { error = "Unknown lock token." });
            }
            await session.Receiver(req.Queue).DeferMessageAsync(msg, cancellationToken: ct);
            return Results.Ok(new { deferred = true, sequenceNumber = msg.SequenceNumber });
        });

        // --- Topic CRUD (proxied) ---
        api.MapGet("/topics", async (string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.GetAsync(Combine(managementUrl, "/topics/"), ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapPut("/topics/{name}", async (string name, CreateTopicProxyRequest body, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var payload = JsonContent.Create(body.Options ?? new());
            var resp = await http.PutAsync(Combine(body.ManagementUrl, $"/topics/{name}"), payload, ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapDelete("/topics/{name}", async (string name, string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.DeleteAsync(Combine(managementUrl, $"/topics/{name}"), ct);
            return Results.StatusCode((int)resp.StatusCode);
        });

        // --- Subscription CRUD (proxied) ---
        api.MapGet("/topics/{topic}/subscriptions", async (string topic, string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.GetAsync(Combine(managementUrl, $"/topics/{topic}/subscriptions"), ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapPut("/topics/{topic}/subscriptions/{name}", async (string topic, string name, CreateSubscriptionProxyRequest body, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var payload = JsonContent.Create(body.Options ?? new());
            var resp = await http.PutAsync(Combine(body.ManagementUrl, $"/topics/{topic}/subscriptions/{name}"), payload, ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapDelete("/topics/{topic}/subscriptions/{name}", async (string topic, string name, string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.DeleteAsync(Combine(managementUrl, $"/topics/{topic}/subscriptions/{name}"), ct);
            return Results.StatusCode((int)resp.StatusCode);
        });

        // --- Rule CRUD (proxied) ---
        api.MapGet("/topics/{topic}/subscriptions/{sub}/rules", async (string topic, string sub, string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.GetAsync(Combine(managementUrl, $"/topics/{topic}/subscriptions/{sub}/rules"), ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapPut("/topics/{topic}/subscriptions/{sub}/rules/{name}", async (string topic, string sub, string name, CreateRuleProxyRequest body, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var payload = JsonContent.Create(body.Rule);
            var resp = await http.PutAsync(Combine(body.ManagementUrl, $"/topics/{topic}/subscriptions/{sub}/rules/{name}"), payload, ct);
            return Results.Content(await resp.Content.ReadAsStringAsync(ct), "application/json", statusCode: (int)resp.StatusCode);
        });

        api.MapDelete("/topics/{topic}/subscriptions/{sub}/rules/{name}", async (string topic, string sub, string name, string managementUrl, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            var resp = await http.DeleteAsync(Combine(managementUrl, $"/topics/{topic}/subscriptions/{sub}/rules/{name}"), ct);
            return Results.StatusCode((int)resp.StatusCode);
        });

        // --- Connectivity check ---
        api.MapPost("/ping", async (PingRequest req, SessionManager sessions, IHttpClientFactory httpFactory, CancellationToken ct) =>
        {
            var http = httpFactory.CreateClient();
            string? mgmtStatus = null;
            string? sdkStatus = null;

            try
            {
                var resp = await http.GetAsync(Combine(req.ManagementUrl, "/health"), ct);
                mgmtStatus = $"{(int)resp.StatusCode} {resp.StatusCode}";
            }
            catch (Exception ex)
            {
                mgmtStatus = "error: " + ex.Message;
            }

            try
            {
                var session = sessions.GetOrCreate(req.ConnectionString);
                // Cheap probe: open a sender then close it. ServiceBusClient opens on first send/receive.
                await using var sender = session.Sender("__ping__probe__");
                sdkStatus = "ok (client constructed; actual AMQP open happens on first send/receive)";
            }
            catch (Exception ex)
            {
                sdkStatus = "error: " + ex.Message;
            }

            return Results.Ok(new { management = mgmtStatus, serviceBus = sdkStatus });
        });

        return endpoints;
    }

    private static string Combine(string baseUrl, string suffix)
        => baseUrl.TrimEnd('/') + suffix;

    private static object ToDto(ServiceBusReceivedMessage msg) => new
    {
        received = true,
        sequenceNumber = Safe(() => (long?)msg.SequenceNumber),
        messageId = msg.MessageId,
        correlationId = msg.CorrelationId,
        subject = msg.Subject,
        contentType = msg.ContentType,
        enqueuedTime = SafeTime(() => msg.EnqueuedTime),
        lockedUntil = SafeTime(() => msg.LockedUntil),
        expiresAt = SafeTime(() => msg.ExpiresAt),  // M6 TTL deadline
        timeToLive = Safe(() => (TimeSpan?)msg.TimeToLive),
        deliveryCount = Safe(() => (int?)msg.DeliveryCount),
        lockToken = msg.LockToken,
        body = SafeBody(msg.Body),
        applicationProperties = msg.ApplicationProperties.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString()),
        // M5: dead-letter metadata — populated only on messages received from a DLQ.
        deadLetterReason = msg.DeadLetterReason,
        deadLetterErrorDescription = msg.DeadLetterErrorDescription,
        deadLetterSource = msg.DeadLetterSource,
    };

    /// <summary>
    /// Several <see cref="ServiceBusReceivedMessage"/> properties dereference Nullable and NRE
    /// if the broker hasn't stamped the corresponding AMQP header/annotation. Wrap to return null
    /// rather than fail the whole response.
    /// </summary>
    private static T? Safe<T>(Func<T?> get) where T : struct
    {
        try { return get(); } catch { return null; }
    }

    /// <summary>
    /// Like <see cref="Safe"/> but also filters out <c>default(DateTimeOffset)</c> (0001-01-01),
    /// which the SDK returns when an annotation is absent. Returning null keeps the UI clean.
    /// </summary>
    private static DateTimeOffset? SafeTime(Func<DateTimeOffset> get)
    {
        try
        {
            var value = get();
            return value == default ? null : value;
        }
        catch { return null; }
    }

    private static string SafeBody(BinaryData body)
    {
        try { return body.ToString(); }
        catch { return $"<{body.ToMemory().Length} bytes>"; }
    }
}

public sealed record CreateQueueRequest(string ManagementUrl, Dictionary<string, object>? Options);
public sealed record CreateTopicProxyRequest(string ManagementUrl, Dictionary<string, object>? Options);
public sealed record CreateSubscriptionProxyRequest(string ManagementUrl, Dictionary<string, object>? Options);
public sealed record CreateRuleProxyRequest(string ManagementUrl, Dictionary<string, object?> Rule);
public sealed record SendRequest(
    string ConnectionString,
    string Queue,
    string? Body,
    string? MessageId,
    string? CorrelationId,
    string? Subject,
    string? ContentType,
    string? ReplyTo,
    string? To,
    string? SessionId,
    string? PartitionKey,
    int? TimeToLiveSeconds,
    DateTimeOffset? ScheduledEnqueueTime,
    Dictionary<string, string>? Properties);
public sealed record ReceiveRequest(string ConnectionString, string Queue, int? TimeoutSeconds);
public sealed record DispositionRequest(string ConnectionString, string Queue, string LockToken);
public sealed record DeadLetterRequest(string ConnectionString, string Queue, string LockToken, string? Reason, string? Description);
public sealed record PingRequest(string ConnectionString, string ManagementUrl);
