#!/bin/sh
# Runs the broker (Host) AND the Explorer side-by-side inside the container.
# POSIX-only sh — no bash dep. Handles graceful SIGTERM/SIGINT and exits the container
# the moment either child process dies (so the orchestrator can restart cleanly).

set -e

# Bind each app to the right Kestrel URL. The two env vars are split so the user can
# override either independently without colliding on ASPNETCORE_URLS.
HOST_URLS="${ASPNETCORE_URLS_HOST:-http://+:5300}"
EXPLORER_URLS="${ASPNETCORE_URLS_EXPLORER:-http://+:5400}"

# Start the broker. Note the `cd` into the publish dir — ASP.NET Core's
# WebApplication.CreateBuilder defaults the content root to the current working
# directory, so running from /app would make appsettings.json + wwwroot resolution miss.
# Pass the bind URL via the standard ASPNETCORE_URLS knob so it overrides the value
# baked into the app's appsettings.json.
( cd /app/host && ASPNETCORE_URLS="$HOST_URLS" exec dotnet OpenServiceBus.Host.dll ) &
HOST_PID=$!
echo "[entrypoint] started broker  (pid=$HOST_PID, urls=$HOST_URLS)"

# Give the broker a brief head start. The Explorer doesn't strictly need it — it talks
# to the broker over the network at runtime, not at startup — but it avoids a noisy
# first-request retry in the UI when the user opens the page immediately.
sleep 0.5

( cd /app/explorer && ASPNETCORE_URLS="$EXPLORER_URLS" exec dotnet OpenServiceBus.Explorer.dll ) &
EXPLORER_PID=$!
echo "[entrypoint] started explorer (pid=$EXPLORER_PID, urls=$EXPLORER_URLS)"

# Forward SIGTERM/SIGINT to both children so `docker stop` shuts down cleanly.
term_handler() {
    echo "[entrypoint] caught signal, forwarding to children"
    kill -TERM "$HOST_PID"     2>/dev/null || true
    kill -TERM "$EXPLORER_PID" 2>/dev/null || true
    wait "$HOST_PID"     2>/dev/null || true
    wait "$EXPLORER_PID" 2>/dev/null || true
    exit 0
}
trap term_handler TERM INT

# Poll for either child to die. POSIX sh has no `wait -n`, so this is the portable spin.
while kill -0 "$HOST_PID" 2>/dev/null && kill -0 "$EXPLORER_PID" 2>/dev/null; do
    sleep 1
done

echo "[entrypoint] one of the children exited, tearing down the other"
kill -TERM "$HOST_PID"     2>/dev/null || true
kill -TERM "$EXPLORER_PID" 2>/dev/null || true
wait "$HOST_PID"     2>/dev/null || true
wait "$EXPLORER_PID" 2>/dev/null || true
exit 1
