# syntax=docker/dockerfile:1
# Multi-stage build for OpenServiceBus.Host. Alpine-based to keep the image small.
# Runs SQLite-backed by default (persistent across container restarts) with the .db file at
# /data/broker.db — mount /data as a volume for durability.
#
# Exposed ports:
#   5672 — AMQP listener (Service Bus SDK + AMQPNetLite clients)
#   5300 — REST management API + /health
#
# Optional environment variables:
#   OPENSERVICEBUS__STORAGE__MODE        InMemory | Sqlite (default: Sqlite in this image)
#   OPENSERVICEBUS__STORAGE__DATASOURCE  Path to the SQLite .db file (default: /data/broker.db)
#   OPENSERVICEBUS_CONFIG                Path to a Microsoft-emulator-compatible config.json
#                                        for bootstrapping queues at startup. Mount it read-only.

# ─── Build stage ──────────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src

# Copy only what NuGet restore needs first, so the layer cache survives source-only edits.
COPY Directory.Build.props Directory.Packages.props OpenServiceBus.slnx ./
COPY src/OpenServiceBus.Core/OpenServiceBus.Core.csproj                 src/OpenServiceBus.Core/
COPY src/OpenServiceBus.InMemoryStorage/OpenServiceBus.InMemoryStorage.csproj  src/OpenServiceBus.InMemoryStorage/
COPY src/OpenServiceBus.SqliteStorage/OpenServiceBus.SqliteStorage.csproj      src/OpenServiceBus.SqliteStorage/
COPY src/OpenServiceBus.Amqp/OpenServiceBus.Amqp.csproj                  src/OpenServiceBus.Amqp/
COPY src/OpenServiceBus.Management/OpenServiceBus.Management.csproj      src/OpenServiceBus.Management/
COPY src/OpenServiceBus.Host/OpenServiceBus.Host.csproj                  src/OpenServiceBus.Host/

RUN dotnet restore src/OpenServiceBus.Host/OpenServiceBus.Host.csproj

# Copy the rest of the source and publish a self-contained-ish framework-dependent build.
COPY src/ src/
RUN dotnet publish src/OpenServiceBus.Host/OpenServiceBus.Host.csproj \
        --configuration Release \
        --no-restore \
        --output /app/publish

# ─── Runtime stage ────────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/aspnet:10.0-alpine AS runtime

# Run as a non-root user — the broker doesn't need root and OCI scanners flag images that do.
RUN addgroup -S osb && adduser -S osb -G osb \
    && mkdir -p /data \
    && chown -R osb:osb /data

USER osb
WORKDIR /app
COPY --from=build /app/publish ./

# Default to SQLite, persisted at /data/broker.db. Override with -e to switch.
ENV OPENSERVICEBUS__STORAGE__MODE=Sqlite \
    OPENSERVICEBUS__STORAGE__DATASOURCE=/data/broker.db \
    ASPNETCORE_URLS=http://+:5300 \
    DOTNET_RUNNING_IN_CONTAINER=true

VOLUME ["/data"]
EXPOSE 5300 5672

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:5300/health || exit 1

ENTRYPOINT ["dotnet", "OpenServiceBus.Host.dll"]
