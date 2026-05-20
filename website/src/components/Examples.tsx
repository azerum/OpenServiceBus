import { useState } from "react";
import CopyButton from "./CopyButton";

type Example = {
  id: string;
  label: string;
  blurb: string;
  language: string;
  code: string;
};

const EXAMPLES: Example[] = [
  {
    id: "docker",
    label: "Docker",
    blurb:
      "Run the broker, management REST API, and Explorer UI in one Alpine container. Best when you want all three reachable from outside your test process.",
    language: "bash",
    code: `# Start the broker, management REST API, and Explorer UI together.
docker run --rm \\
  -p 5672:5672 \\   # AMQP broker
  -p 5300:5300 \\   # Management REST API
  -p 5400:5400 \\   # Explorer UI
  mauritsarissen/openservicebus:latest

# Point the Azure SDK at:
#   Endpoint=sb://localhost;SharedAccessKeyName=x;SharedAccessKey=x;UseDevelopmentEmulator=true
#
# Open the Explorer:
#   http://localhost:5400`,
  },
  {
    id: "xunit",
    label: "xUnit fixture",
    blurb:
      "Embed the broker directly in your test process via the OpenServiceBus.Testing NuGet package. No Docker, no external dependencies, ~50ms startup.",
    language: "csharp",
    code: `using Azure.Messaging.ServiceBus;
using OpenServiceBus.Testing;

public class OrderProcessorTests : IAsyncLifetime
{
    private OpenServiceBusTestHost _host = null!;

    public async Task InitializeAsync()
    {
        // Spins up an in-memory broker on a random free port and returns
        // a ready-to-use connection string. ~50ms.
        _host = await OpenServiceBusTestHost.StartAsync();
        await _host.CreateQueueAsync("orders");
    }

    public Task DisposeAsync() => _host.DisposeAsync().AsTask();

    [Fact]
    public async Task SendAndReceive_RoundTrip()
    {
        await using var client = new ServiceBusClient(_host.ConnectionString);

        var sender = client.CreateSender("orders");
        await sender.SendMessageAsync(new ServiceBusMessage("order-42"));

        var receiver = client.CreateReceiver("orders");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));

        Assert.Equal("order-42", msg.Body.ToString());
        await receiver.CompleteMessageAsync(msg);
    }
}`,
  },
  {
    id: "dotnet",
    label: ".NET console",
    blurb:
      "Plain old console app talking to a broker started via Docker. Same code you'd write against the real Azure Service Bus - only the connection string changes.",
    language: "csharp",
    code: `using Azure.Messaging.ServiceBus;

const string conn =
    "Endpoint=sb://localhost;" +
    "SharedAccessKeyName=RootManageSharedAccessKey;" +
    "SharedAccessKey=anykey;" +
    "UseDevelopmentEmulator=true";

await using var client = new ServiceBusClient(conn);

// --- Sender ---
var sender = client.CreateSender("orders");
await sender.SendMessageAsync(new ServiceBusMessage("hello"));
Console.WriteLine("Sent.");

// --- Receiver (peek-lock) ---
var receiver = client.CreateReceiver("orders");
var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

if (msg is null)
{
    Console.WriteLine("No messages.");
    return;
}

Console.WriteLine($"Received: {msg.Body}");
await receiver.CompleteMessageAsync(msg);`,
  },
  {
    id: "functions",
    label: "Azure Functions",
    blurb:
      "Isolated-worker Functions app with a ServiceBusTrigger pointed at OpenServiceBus. Verified end-to-end in the project's integration test suite.",
    language: "csharp",
    code: `using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

public class OrderTrigger
{
    [Function(nameof(OrderTrigger))]
    public async Task Run(
        [ServiceBusTrigger("orders", Connection = "ServiceBusConnection")]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        FunctionContext context)
    {
        var logger = context.GetLogger<OrderTrigger>();
        logger.LogInformation("Processing: {Body}", message.Body);

        // ... your business logic ...

        await actions.CompleteMessageAsync(message);
    }
}

// local.settings.json
// {
//   "Values": {
//     "AzureWebJobsStorage": "UseDevelopmentStorage=true",
//     "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
//     "ServiceBusConnection":
//       "Endpoint=sb://localhost;SharedAccessKeyName=x;SharedAccessKey=x;UseDevelopmentEmulator=true"
//   }
// }`,
  },
];

export default function Examples() {
  const [activeId, setActiveId] = useState(EXAMPLES[0].id);
  const active = EXAMPLES.find((e) => e.id === activeId)!;

  return (
    <section className="py-12">
      {/* Tab strip */}
      <div className="flex flex-wrap gap-1 rounded-lg border border-neutral-800 bg-neutral-900/40 p-1 mb-6 w-fit max-w-full">
        {EXAMPLES.map((ex) => (
          <button
            key={ex.id}
            onClick={() => setActiveId(ex.id)}
            className={`px-3.5 py-2 rounded-md text-sm font-medium transition ${
              activeId === ex.id
                ? "bg-neutral-800 text-white"
                : "text-neutral-400 hover:text-neutral-100 hover:bg-neutral-900"
            }`}
          >
            {ex.label}
          </button>
        ))}
      </div>

      {/* Blurb */}
      <p className="mb-4 text-neutral-400 max-w-2xl">{active.blurb}</p>

      {/* Code block */}
      <div className="relative rounded-xl border border-neutral-800 bg-neutral-900/60 p-4 sm:p-5">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-red-500/70" />
            <span className="h-2.5 w-2.5 rounded-full bg-amber-500/70" />
            <span className="h-2.5 w-2.5 rounded-full bg-emerald-500/70" />
            <span className="ml-2 text-xs text-neutral-500 font-mono">{active.language}</span>
          </div>
          <CopyButton text={active.code} />
        </div>
        <pre className="overflow-x-auto font-mono text-[12.5px] sm:text-sm leading-relaxed text-neutral-100">
          <Code text={active.code} />
        </pre>
      </div>
    </section>
  );
}

// Tiny manual highlighter - comments grey, strings green, keywords violet.
// Keeps the bundle small (no Shiki/Prism) and works fine for the short snippets we ship.
function Code({ text }: { text: string }) {
  const lines = text.split("\n");
  return (
    <>
      {lines.map((line, i) => (
        <div key={i}>{highlight(line)}{"\n"}</div>
      ))}
    </>
  );
}

const KEYWORDS = new Set([
  "using",
  "public",
  "private",
  "static",
  "async",
  "await",
  "class",
  "new",
  "var",
  "const",
  "return",
  "if",
  "else",
  "true",
  "false",
  "null",
  "void",
  "Task",
  "string",
  "int",
  "bool",
]);

function highlight(line: string) {
  // Comments - anything after // (bash and C# both use this); also full-line # for shell.
  const commentSplit = line.match(/^(.*?)(\/\/.*|#.*)?$/);
  const codePart = commentSplit?.[1] ?? line;
  const commentPart = commentSplit?.[2] ?? "";

  // Tokenize the non-comment part naively: strings ("...") + word boundaries.
  const tokens: React.ReactNode[] = [];
  const re = /("(?:[^"\\]|\\.)*"|\b[A-Za-z_][A-Za-z0-9_]*\b|.)/g;
  let m: RegExpExecArray | null;
  let key = 0;
  while ((m = re.exec(codePart)) !== null) {
    const tok = m[0];
    if (tok.startsWith('"')) {
      tokens.push(<span key={key++} className="text-emerald-400">{tok}</span>);
    } else if (KEYWORDS.has(tok)) {
      tokens.push(<span key={key++} className="text-violet-300">{tok}</span>);
    } else {
      tokens.push(<span key={key++}>{tok}</span>);
    }
  }
  return (
    <>
      {tokens}
      {commentPart && <span className="text-neutral-500">{commentPart}</span>}
    </>
  );
}
