# labeler-relay

labeler-relay combines AT Protocol labeler streams into one WebSocket stream for applications that consume labels. Consumers track one relay sequence number instead of a separate cursor for each labeler.

## Install and run

The instructions below build from source. You need Go 1.27.3 or later, Git, curl, and a Bash-compatible terminal. The repository does not specify a supported operating-system matrix. These commands use Unix shell syntax.

No Bluesky account is required. The relay needs network access to the firehose, DID documents, and upstream labelers. A DID is an AT Protocol account identifier.

1. Open a terminal in the folder where you want to keep the source.
2. Download the source and enter its folder:

   ```bash
   git clone https://github.com/skywatch-bsky/labeler-relay.git
   cd labeler-relay
   ```

3. Build the executable. Go downloads the required dependencies:

   ```bash
   go build -o labeler-relay ./cmd/labeler-relay
   ```

4. Enter a private admin token at the prompt. Choose a long, random value and keep it in your password manager.

   ```bash
   read -r -s -p 'Admin token: ' LABELER_RELAY_ADMIN_TOKEN
   printf '\n'
   export LABELER_RELAY_ADMIN_TOKEN
   ```

5. Start the relay on the local interface:

   ```bash
   LABELER_RELAY_LISTEN_ADDR=127.0.0.1:8080 ./labeler-relay
   ```

   The relay creates `labeler-relay.db` in the current folder and initializes its schema. No separate database server is required.
   It watches the firehose for labeler service declarations in the `app.bsky.labeler.service`, `town.delve.labeler.service`, and `so.sprk.labeler.service` collections, and enables discovered labelers by default.
   Discovery observes new firehose events. It is not an initial inventory of all existing labelers.

6. Open another terminal and check the relay:

   ```bash
   curl --fail http://localhost:8080/_health
   ```

   A successful response is a JSON object with `head_seq`, `labeler_count`, `retention_floor`, and `retention_window_seconds`.
   Counts can be zero before discovery receives events. Use the [admin API](#admin-api) to register a known labeler.

7. To stop the relay, press Ctrl+C in its terminal.

## Developer setup

Use Go 1.27.3 or later, Git, and Make. Follow the source-download steps above, then run these commands from the repository root:

```bash
go mod download
make build
make test
make lint
```

`make build` checks all packages with `go build ./...`. It does not create the executable used in the installation steps.

Set `LABELER_RELAY_ADMIN_TOKEN` as shown above, then run:

```bash
LABELER_RELAY_LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/labeler-relay
```

Check `/_health` for the JSON response described above. SQLite initializes automatically. Keep the database directory writable.
The configuration loader reads environment variables, not a `.env` file. Keep tokens out of tracked files.

| Command | Purpose |
|---------|---------|
| `make build` | Build all packages. |
| `make test` | Run unit tests. |
| `make lint` | Run `go vet ./...`. |
| `make test-integration` | Run `internal/e2e/` tests with the `integration` tag and a five-minute timeout. Requires network access. |
| `make cborgen` | Regenerate CBOR marshalling code, then build all packages. |

Do not edit generated `cbor_gen.go` files by hand. Node.js and npm are optional tools for the [stream client](#stream-client).
Bash, Python 3, and curl are required only for the [bulk importer](#bulk-import).

## What it does

The relay subscribes to enabled labelers through `com.atproto.label.subscribeLabels` and publishes events through `community.labeler.sync.subscribeLabelers`.
It discovers labelers from declaration records in the `app.bsky.labeler.service`, `town.delve.labeler.service`, and `so.sprk.labeler.service` firehose collections, or accepts manual registration through the admin API.

Relayed labels retain their signatures and signed fields. Consumers can verify them against the origin labeler's `#atproto_label` signing key.
The signature policy rejects missing signatures. It does not establish that a signature is valid.

## Configuration

All configuration uses environment variables with the `LABELER_RELAY_` prefix.

| Variable | Default | Description |
|----------|---------|-------------|
| `LABELER_RELAY_DB_PATH` | `labeler-relay.db` | SQLite database path. |
| `LABELER_RELAY_LISTEN_ADDR` | `:8080` | HTTP listen address. The default listens on all interfaces. |
| `LABELER_RELAY_FIREHOSE_URL` | `wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos` | AT Protocol firehose URL. Must use `ws://` or `wss://`; any other value is refused at startup. |
| `LABELER_RELAY_ADMIN_TOKEN` | Required | Bearer token for the `/admin/` API. Must not be empty. |
| `LABELER_RELAY_RETENTION_WINDOW` | `336h` (14 days) | Event retention period. Accepts Go durations or whole days such as `7d`. Must be positive. |
| `LABELER_RELAY_REQUIRE_SIG` | `true` | Reject labels without signatures. Supports per-labeler overrides through the admin API. |
| `LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED` | `true` | Enable newly discovered labelers. |
| `LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC` | `500` | Positive integer limit per labeler, in labels per second. |
| `LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR` | `100000` | Positive integer limit per labeler, in labels per hour. |
| `LABELER_RELAY_QUERY_RATE_LIMIT_PER_SEC` | `10` | Positive integer limit per client IP, in query requests per second. |
| `LABELER_RELAY_QUERY_RATE_LIMIT_PER_HOUR` | `1200` | Positive integer limit per client IP, in query requests per hour. |
| `LABELER_RELAY_TRUSTED_PROXIES` | `127.0.0.1/32,::1/128` | Comma-separated CIDR ranges for trusted proxies. The query rate limiter uses `X-Forwarded-For` only from these proxies. An empty value or `none` disables proxy trust. |
| `LABELER_RELAY_SUBSCRIBER_BUF_SIZE` | `512` | Event buffer per downstream subscriber. Integer from 1 through 65536. |

Unset optional variables use their defaults. Empty values also use defaults, except for `LABELER_RELAY_TRUSTED_PROXIES`: an empty value disables proxy trust, as does `none`. Use `true` or `false` for boolean settings.
Invalid boolean, integer, duration, or CIDR values prevent startup. Slow consumers can lose their connection when their buffer fills.

Retention is always enabled. An unset `LABELER_RELAY_RETENTION_WINDOW` uses the 14-day default, and a window of zero or less is rejected at startup; there is no keep-forever mode. Set a very large window, such as `9999d` (about 27 years), to retain events far longer than the default.

## Endpoints

### WebSocket: subscribe to labels

```text
ws://localhost:8080/xrpc/community.labeler.sync.subscribeLabelers
ws://localhost:8080/xrpc/community.labeler.sync.subscribeLabelers?cursor=100
```

Without a cursor, the connection starts at the current stream head. With `cursor=N`, it replays retained events with `seq > N`, then continues live.
Save the last processed relay `seq` to resume after a disconnect.

Each binary WebSocket message contains two concatenated CBOR objects: an XRPC header and a body. CBOR is a binary data format.
Message headers use `{"op":1,"t":"#labels"}`, `#service`, or `#info`. Error headers use `{"op":-1}`.
The JSON below illustrates decoded bodies, not JSON messages sent over the socket.

**`#labels`** carries a batch from an upstream labeler:

```json
{
  "seq": 42,
  "src": "did:plc:ar7c4by46qjdydhdevvrndac",
  "upstreamSeq": 35916330,
  "labels": [
    {
      "src": "did:plc:ar7c4by46qjdydhdevvrndac",
      "uri": "at://did:plc:example/app.bsky.feed.post/abc123",
      "val": "spam",
      "cts": "2026-06-02T14:05:50.164Z",
      "ver": 1,
      "sig": { "$bytes": "base64..." }
    }
  ]
}
```

`seq` is the relay sequence. `upstreamSeq` is the upstream sequence. Each item in `labels` is a `com.atproto.label.defs#label`.
`$bytes` illustrates binary signature bytes in JSON. The sample signature is a placeholder.

**`#service`** carries a labeler service declaration observed through the firehose:

```json
{
  "seq": 43,
  "src": "did:plc:example",
  "op": "create",
  "collection": "app.bsky.labeler.service",
  "record": {
    "$type": "app.bsky.labeler.service",
    "policies": { "labelValues": ["spam"] },
    "createdAt": "2026-06-01T00:00:00Z"
  }
}
```

`op` is `create`, `update`, or `delete`. `collection` names the declaration collection the record belongs to: `app.bsky.labeler.service`, `town.delve.labeler.service`, or `so.sprk.labeler.service`. Delete events carry the deleted `collection` and omit `record`.

The full declaration record accompanies create and update events as the raw CBOR block from the labeler's repository, passed through byte-faithfully: its bytes are exactly the upstream block, never decoded and re-encoded, including any fields this relay does not know. Frames recorded before multi-collection support carry no `collection`; those records are `app.bsky.labeler.service`.

A discovered labeler that deletes its last declaration record is considered inactive: the relay disables it and unsubscribes. A later declaration re-enables it. A labeler an operator disabled through the admin API stays disabled.

**`#info`** reports an outdated cursor:

```json
{
  "name": "OutdatedCursor",
  "message": "cursor is below the retention floor; resuming from the oldest available event"
}
```

When retained events exist, a cursor below `retention_floor - 1` receives this message, and playback resumes at the oldest retained event.
A cursor equal to `retention_floor - 1` starts there without an info message.
On an empty store, cursors at or below zero are accepted without an info message, and there is nothing to backfill; positive cursors receive `FutureCursor`.

Error bodies contain `error` and `message`. The relay closes the connection after `InvalidRequest` for a malformed cursor or `FutureCursor` for a cursor above the head.
It attempts to send `ConsumerTooSlow` before closing a consumer whose buffer overflows. Socket write failures can close the connection without an error frame.

### Query labelers (XRPC)

Two GET endpoints expose the registry read-only. No authentication is required. Requests are rate limited per client IP: 10 per second and 1200 per hour by default. Both limits are configurable (see the configuration table).
The rate limiter uses the connection peer IP and ignores `X-Forwarded-For` unless that peer matches `LABELER_RELAY_TRUSTED_PROXIES` (loopback by default). For trusted proxies, it reads `X-Forwarded-For` from right to left and uses the first IP outside the trusted ranges. If the header is missing, all entries are trusted, or the resolver encounters a malformed entry, it uses the proxy IP. These requests share one rate-limit bucket for that proxy.

**List labelers:**

```bash
curl --fail 'http://localhost:8080/xrpc/community.labeler.sync.getLabelers?limit=2'
```

Example response:

```json
{
  "labelers": [
    {
      "id": 1,
      "did": "did:plc:ar7c4by46qjdydhdevvrndac",
      "createdAt": "2026-05-01T00:00:00Z",
      "updatedAt": "2026-06-01T00:00:00Z",
      "endpoint": "bsky.app",
      "enabled": true,
      "last_upstream_seq": 35916330,
      "declarations": ["app.bsky.labeler.service", "town.delve.labeler.service"]
    },
    {
      "id": 2,
      "did": "did:plc:k2vjpxvpxbnfyjzkb66cg7ju",
      "createdAt": "2026-05-02T00:00:00Z",
      "endpoint": "",
      "enabled": false,
      "last_upstream_seq": null,
      "declarations": []
    }
  ],
  "cursor": 2
}
```

`labelers` lists registry entries oldest-observed first: the order in which the relay first observed them, not DID order.
`id` is the entry's monotonically increasing identifier, minted when it was first registered and never reused. `createdAt` is when the relay first observed a declaration for the DID. `updatedAt` is the date of the most recently observed declaration update; it is absent when this relay has not observed a declaration for the labeler.
`endpoint` is the labeler's host, including its port when the DID document specifies one. It stays empty until the relay resolves the labeler's DID document. `last_upstream_seq` is the last sequence ingested from that labeler; it is `null` when the labeler has never ingested.
`declarations` lists the declaration collections the labeler currently holds, as observed by this relay. It is empty (never null) for manually added labelers.
`cursor` is the `id` of the last labeler on the page. Pass it back to fetch the next page; a response without `cursor` is the last page.

Parameters:

- `limit` (optional, default `50`): integer from 1 through 100. Out-of-range or non-integer values return HTTP 400 `InvalidRequest`. Values are rejected, not clamped.
- `cursor` (optional): the `id` of the last labeler on the previous page. A non-negative integer; non-integer or negative values return HTTP 400 `InvalidRequest`. A cursor past the last labeler returns an empty list, so walking past the end is safe.
- `enabled` (optional): `true` returns only enabled labelers, `false` only disabled ones. Omit it to receive all labelers.

This loop walks every page:

```bash
cursor=""
while :; do
  resp=$(curl -s --fail "http://localhost:8080/xrpc/community.labeler.sync.getLabelers?limit=100${cursor:+&cursor=$cursor}")
  jq -r '.labelers[].did' <<<"$resp"
  cursor=$(jq -r '.cursor // empty' <<<"$resp")
  [ -z "$cursor" ] && break
done
```

The cursor is a JSON number; `jq -r` passes it through unchanged.

**Look up one labeler:**

```bash
curl --fail 'http://localhost:8080/xrpc/community.labeler.sync.getLabeler?did=did:plc:ar7c4by46qjdydhdevvrndac'
```

Example response:

```json
{
  "id": 1,
  "did": "did:plc:ar7c4by46qjdydhdevvrndac",
  "createdAt": "2026-05-01T00:00:00Z",
  "updatedAt": "2026-06-01T00:00:00Z",
  "endpoint": "bsky.app",
  "enabled": true,
  "last_upstream_seq": 35916330,
  "declarations": ["app.bsky.labeler.service", "town.delve.labeler.service"]
}
```

The `did` parameter is required. A malformed or unknown DID returns HTTP 400 `InvalidRequest`. The response carries the same fields as each `labelers` entry above, including the optional `updatedAt` and the `declarations` array.

Error responses are JSON objects with `error` and `message`:

- HTTP 400 `InvalidRequest`: a malformed parameter, such as an out-of-range `limit`, a non-integer or negative `cursor`, an `enabled` value other than `true` or `false`, or a missing, malformed, or unknown `did`.
- HTTP 429 `RateLimitExceeded`: the per-client-IP limit is exhausted. A `Retry-After: 1` header is a retry hint; an exhausted hourly budget can outlast it. For a reverse proxy, set `LABELER_RELAY_TRUSTED_PROXIES` to its CIDR ranges so the limiter can use client IPs from `X-Forwarded-For`. The default covers a same-host proxy, such as Caddy, that connects over loopback.
- HTTP 500 `InternalServerError`: a registry read failed on the server.

GET is the expected method and HEAD is also accepted. Other methods receive HTTP 405 with an `Allow` header.

### Health check

```bash
curl --fail http://localhost:8080/_health
```

Example response:

```json
{
  "head_seq": 2061,
  "labeler_count": 415,
  "retention_floor": 1,
  "retention_window_seconds": 1209600
}
```

The endpoint returns HTTP 200 on success or HTTP 500 if a store read fails.
`labeler_count` includes disabled registry entries. It is not the number of connected upstreams.

### Metrics

```text
GET /metrics
```

Prometheus metrics include the head sequence, retention floor, per-labeler ingest counters, dropped unsigned labels, consumer count, connected upstreams, and throttle counters.

### Admin API

All admin endpoints require `Authorization: Bearer <ADMIN_TOKEN>`.
Set `LABELER_RELAY_ADMIN_TOKEN` in the terminal used for these commands, using the same token as the running relay.
Replace `{did}` in paths with the labeler's DID.

**Add a labeler:**

```bash
curl --fail -X POST http://localhost:8080/admin/labelers \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"did": "did:plc:ar7c4by46qjdydhdevvrndac"}'
```

The relay resolves the endpoint from the DID document and adds or enables the registry entry. HTTP 201 returns the stored entry.

**List labelers:**

```bash
curl --fail http://localhost:8080/admin/labelers \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN"
```

**Disable or enable a labeler:**

```bash
curl --fail -X POST 'http://localhost:8080/admin/labelers/{did}/disable' \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN"

curl --fail -X POST 'http://localhost:8080/admin/labelers/{did}/enable' \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN"
```

`DELETE /admin/labelers/{did}` also disables the labeler. It does not delete the registry entry or retained events.
These three operations return HTTP 204 on success or HTTP 404 for an unknown DID.

**Set the signature requirement for one labeler:**

```bash
curl --fail -X PATCH 'http://localhost:8080/admin/labelers/{did}' \
  -H "Authorization: Bearer $LABELER_RELAY_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"require_sig": true}'
```

Use `false` to allow unsigned labels or `null` to restore the global default. The `require_sig` field is required.
HTTP 200 returns the stored entry. A policy change restarts the upstream subscription.

## Bulk import

`scripts/import-labelers.sh` registers labelers through the admin API. It requires Bash, Python 3, and curl.
Run it from the repository root with the admin token already exported:

```bash
./scripts/import-labelers.sh labelers.csv http://localhost:8080
```

Arguments are the CSV path, optional relay URL, and optional admin token. The URL defaults to `http://localhost:8080`.
If the third argument is absent, the script uses `LABELER_RELAY_ADMIN_TOKEN`. Prefer the environment variable to avoid placing the token in command history.

The CSV must contain `did` and `visibility` columns. The script imports only rows with a nonempty DID and `visibility` equal to `Declared`.
It strips surrounding whitespace from both values. Without the visibility column, it imports no rows.

## Stream client

`utils/stream-client.ts` decodes the binary stream. It requires Node.js and npm. From the repository root:

```bash
cd utils
npm ci
npx tsx stream-client.ts
```

The client accepts an optional URL followed by an optional cursor. It has no named flags.

```bash
npx tsx stream-client.ts ws://relay.example.com/xrpc/community.labeler.sync.subscribeLabelers
npx tsx stream-client.ts ws://localhost:8080/xrpc/community.labeler.sync.subscribeLabelers 100
```

When supplying a separate cursor, use a URL without an existing query string.
Label and service events go to stdout as newline-delimited JSON. Connection status, info frames, and errors go to stderr.
The client changes `#labels` into `type: "label"` and nests the batch in `record`, with the upstream sequence in `record.seq`.
It renders deleted service records as `record: null`. These are client output conventions, not the wire format.

## Production deployment

The relay serves unencrypted HTTP and WebSocket connections. Use a reverse proxy with TLS termination for HTTPS and `wss://` access.
Restrict direct access to the relay port. The stream, query, health, and metrics endpoints do not require the admin token.

SQLite uses WAL mode automatically. Use persistent storage and run one relay process per database.
Size storage and measure throughput for your workload. The default retention period is 14 days.

## Lexicon

The subscription schema is [subscribeLabelers.json](lexicons/community/labeler/sync/subscribeLabelers.json).
It defines the `community.labeler.sync.subscribeLabelers` message bodies and cursor parameter.
The query schemas are [getLabelers.json](lexicons/community/labeler/sync/getLabelers.json) and [getLabeler.json](lexicons/community/labeler/sync/getLabeler.json).
They define the parameters and response bodies of the query endpoints.
The response object shared by both queries, `labelerView`, is defined once in [defs.json](lexicons/community/labeler/sync/defs.json).

## License

[MIT](LICENSE)
