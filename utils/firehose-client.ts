#!/usr/bin/env tsx
import { WebSocket } from "ws";
import { decodeAll } from "@atproto/lex-cbor";

const url =
  process.argv[2] ??
  "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";

const cursor = process.argv[3];
const connectUrl = cursor ? `${url}?cursor=${cursor}` : url;

const filter = process.argv[4];

process.stderr.write(`Connecting to ${connectUrl}...\n`);
if (filter) {
  process.stderr.write(`Filtering for collection: ${filter}\n`);
}

const ws = new WebSocket(connectUrl);

function toJSON(val: unknown): unknown {
  if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
    if (val.length > 1024) {
      return `<${val.length} bytes>`;
    }
    return { $bytes: Buffer.from(val).toString("base64") };
  }
  if (Array.isArray(val)) {
    return val.map(toJSON);
  }
  if (val !== null && typeof val === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(val as Record<string, unknown>)) {
      if (v !== undefined && v !== null) {
        out[k] = toJSON(v);
      }
    }
    return out;
  }
  return val;
}

let count = 0;

ws.on("open", () => {
  process.stderr.write("Connected.\n");
});

ws.on("message", (data: Buffer) => {
  const items = Array.from(decodeAll(data));
  const [header, body] = items as [
    Record<string, unknown>,
    Record<string, unknown>,
  ];

  const op = header["op"] as number;
  const rawType = (header["t"] as string) ?? "";
  const type = rawType.replace(/^#/, "");

  if (op === -1) {
    process.stderr.write(
      `Error: ${body["error"]} — ${body["message"]}\n`,
    );
    return;
  }

  if (filter && type === "commit") {
    const ops = body["ops"] as Array<Record<string, unknown>> | undefined;
    if (ops && !ops.some((o) => (o["path"] as string)?.startsWith(filter + "/"))) {
      return;
    }
  }

  const line = JSON.stringify({ type, ...toJSON(body) as object });
  process.stdout.write(line + "\n");

  count++;
  if (count % 10000 === 0) {
    process.stderr.write(`${count} events processed\n`);
  }
});

ws.on("close", (code, reason) => {
  process.stderr.write(`Disconnected: ${code} ${reason.toString()}\n`);
  process.exit(0);
});

ws.on("error", (err) => {
  process.stderr.write(`Error: ${err.message}\n`);
  process.exit(1);
});
