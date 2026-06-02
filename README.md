# labeler-relay

A relay that aggregates multiple AT Protocol labeler streams into a single unified output. Instead of connecting to each labeler individually, consumers subscribe to one endpoint and receive all labels re-sequenced under a single monotonic cursor.

## What it does

Every labeler on Bluesky exposes a `com.atproto.label.subscribeLabels` WebSocket. The relay connects to all of them simultaneously, ingests their label events, and rebroadcasts them as one stream under `community.labeler.sync.subscribeLabelers`. Downstream consumers track a single cursor regardless of how many upstream labelers exist.

Labels are passed through byte-faithfully — signatures and all signed fields are identical to what the origin labeler emitted. Consumers can still verify any label against the origin labeler's `#atproto_label` signing key.

Labelers are discovered automatically by watching the AT Protocol firehose for `app.bsky.labeler.service` records, or registered manually via an admin API.

## Quick start

```bash
# Build
make build

# Run (admin token is required)
LABELER_RELAY_ADMIN_TOKEN=your-secret-token ./labeler-relay
```

The relay starts on `:8080` by default. It immediately begins watching the firehose for labeler discovery and will auto-subscribe to any labeler it finds.

## Configuration

All configuration is via environment variables, prefixed `LABELER_RELAY_`.

| Variable | Default | Description |
|----------|---------|-------------|
| `LABELER_RELAY_DB_PATH` | `labeler-relay.db` | SQLite database path. |
| `LABELER_RELAY_LISTEN_ADDR` | `:8080` | HTTP listen address. |
| `LABELER_RELAY_FIREHOSE_URL` | `wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos` | AT Protocol firehose URL. |
| `LABELER_RELAY_ADMIN_TOKEN` | *(required)* | Bearer token for the `/admin/` API. |
| `LABELER_RELAY_RETENTION_WINDOW` | `336h` (14 days) | How long events are kept. Supports Go durations and `Nd` shorthand (e.g. `7d`). |
| `LABELER_RELAY_REQUIRE_SIG` | `true` | Drop unsigned labels by default. Per-labeler overrides are possible. |
| `LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED` | `true` | Auto-enable labelers discovered via firehose. |
| `LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC` | `50` | Per-labeler ingest rate limit (labels/sec). |
| `LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR` | `10000` | Per-labeler ingest rate limit (labels/hour). |

## Endpoints

### WebSocket: subscribe to labels

```
ws://localhost:8080/xrpc/community.labeler.sync.subscribeLabelers
ws://localhost:8080/xrpc/community.labeler.sync.subscribeLabelers?cursor=100
```

Connect with no cursor for live-only. Connect with `cursor=N` to backfill from `seq > N`, then seamlessly continue live. The stream emits three frame types:

**`label`** — relayed labels from an upstream labeler:

```json
{
  "type": "label",
  "seq": 42,
  "src": "did:plc:ar7c4by46qjdydhdevvrndac",
  "record": {
    "$type": "com.atproto.label.subscribeLabels#labels",
    "seq": 35916330,
    "labels": [
      {
        "src": "did:plc:ar7c4by46qjdydhdevvrndac",
        "uri": "at://did:plc:example/app.bsky.feed.post/abc123",
        "cid": "bafyrei...",
        "val": "porn",
        "cts": "2026-06-02T14:05:50.164Z",
        "ver": 1,
        "sig": { "$bytes": "base64..." }
      }
    ]
  }
}
```

Top-level `seq` is the relay-minted sequence. `record.seq` is the upstream labeler's sequence. Each label inside `record.labels` is a complete `com.atproto.label.defs#label` passed through with all fields intact.

**`service`** — a labeler's service declaration was created, updated, or deleted:

```json
{
  "type": "service",
  "seq": 43,
  "src": "did:plc:example",
  "op": "create",
  "record": {
    "$type": "app.bsky.labeler.service",
    "policies": { "labelValues": ["spam", "nsfw"], "labelValueDefinitions": [...] },
    "createdAt": "2026-06-01T00:00:00Z",
    "reasonTypes": [...],
    "subjectTypes": [...],
    "subjectCollections": [...]
  }
}
```

The record is a complete `app.bsky.labeler.service` with all fields. `record` is null for `"op": "delete"`.

**`info`** — control frame (e.g. cursor below the retention floor):

```json
{
  "type": "info",
  "name": "OutdatedCursor",
  "message": "cursor is before retention floor"
}
```

### Health check

```bash
curl http://localhost:8080/_health
```

Returns:

```json
{
  "head_seq": 2061,
  "labeler_count": 415,
  "retention_floor": 1,
  "retention_window_seconds": 1209600
}
```

### Metrics

```
GET /metrics
```

Prometheus-format metrics: head seq, per-labeler ingest rates, dropped unsigned counts, consumer count, connected upstreams, throttle events.

### Admin API

All admin endpoints require `Authorization: Bearer <ADMIN_TOKEN>`.

**Add a labeler:**

```bash
curl -X POST http://localhost:8080/admin/labelers \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"did": "did:plc:ar7c4by46qjdydhdevvrndac"}'
```

The relay resolves the labeler's endpoint from its DID document automatically.

**List labelers:**

```bash
curl http://localhost:8080/admin/labelers \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN"
```

**Disable/enable a labeler:**

```bash
curl -X POST http://localhost:8080/admin/labelers/{did}/disable \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN"

curl -X POST http://localhost:8080/admin/labelers/{did}/enable \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN"
```

**Remove a labeler:**

```bash
curl -X DELETE http://localhost:8080/admin/labelers/{did} \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN"
```

## Bulk import

The `scripts/import-labelers.sh` script reads a CSV of labelers and registers them via the admin API:

```bash
./scripts/import-labelers.sh labelers.csv http://localhost:8080 your-admin-token
```

The CSV needs a `did` column and optionally a `visibility` column. If `visibility` is present, only rows with `visibility=Declared` are imported.

## Stream client

A TypeScript utility for decoding the relay's output stream is included in `utils/`:

```bash
cd utils && npm install

# Live tail
npx tsx stream-client.ts

# Custom URL
npx tsx stream-client.ts ws://relay.example.com/xrpc/community.labeler.sync.subscribeLabelers

# Resume from cursor
npx tsx stream-client.ts ws://localhost:8080/xrpc/community.labeler.sync.subscribeLabelers 100
```

Outputs NDJSON to stdout (pipe to `jq`, log files, etc.). Connection status and errors go to stderr.

## Production deployment

The relay serves `ws://` only. For `wss://`, put a reverse proxy in front (nginx, caddy, etc.) that terminates TLS and forwards to the relay's listen address.

The SQLite database handles the write throughput of hundreds of labelers comfortably. WAL mode is enabled automatically.

## Development

```bash
make build          # Build all packages
make test           # Run unit tests
make test-integration  # Run E2E tests (requires network)
make cborgen        # Regenerate CBOR marshalling code
make lint           # Run go vet
```

## Lexicon

The output subscription lexicon is defined in `lexicons/community/labeler/sync/subscribeLabelers.json`. It follows AT Protocol conventions: CBOR-encoded frames over WebSocket with `{op, t}` headers.

## License

MIT
