#!/usr/bin/env tsx
import { WebSocket } from "ws";
import { decodeFirst, isBytes, fromBytes, isCidLink } from "@atcute/cbor";

const url =
  process.argv[2] ??
  "ws://localhost:8080/xrpc/community.labeler.sync.subscribeLabelers";

const cursor = process.argv[3];
const connectUrl = cursor ? `${url}?cursor=${cursor}` : url;

process.stderr.write(`Connecting to ${connectUrl}...\n`);

const ws = new WebSocket(connectUrl);

function toJSON(val: unknown): unknown {
  if (isBytes(val)) {
    const buf = fromBytes(val as any);
    return { $bytes: Buffer.from(buf).toString("base64") };
  }
  if (isCidLink(val)) {
    return (val as any).toJSON();
  }
  if (Array.isArray(val)) {
    return val.map(toJSON);
  }
  if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
    if (val.length > 1024) {
      return `<${val.length} bytes>`;
    }
    return { $bytes: Buffer.from(val).toString("base64") };
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

ws.on("open", () => {
  process.stderr.write("Connected.\n");
});

ws.on("message", (data: Buffer) => {
  const buf = new Uint8Array(data);
  const [header, remainder] = decodeFirst(buf) as [Record<string, unknown>, Uint8Array];
  const [body] = decodeFirst(remainder) as [Record<string, unknown>, Uint8Array];

  const op = header["op"] as number;
  const rawType = (header["t"] as string) ?? "";
  const type = rawType.replace(/^#/, "");

  if (op === -1) {
    process.stderr.write(
      `Error frame: ${body["error"]} — ${body["message"]}\n`,
    );
    return;
  }

  switch (type) {
    case "labels": {
      const { seq, src, upstreamSeq, labels } = body as {
        seq: number;
        src: string;
        upstreamSeq: number;
        labels: Array<Record<string, unknown>>;
      };
      const line = JSON.stringify({
        type: "label",
        seq,
        src,
        record: {
          $type: "com.atproto.label.subscribeLabels#labels",
          seq: upstreamSeq,
          labels: labels.map((l) => toJSON(l)),
        },
      });
      process.stdout.write(line + "\n");
      break;
    }

    case "service": {
      const { seq, src, op: serviceOp, record } = body as {
        seq: number;
        src: string;
        op: string;
        record: Record<string, unknown> | null;
      };
      const line = JSON.stringify({
        type: "service",
        seq,
        src,
        op: serviceOp,
        record: record
          ? { $type: "app.bsky.labeler.service", ...toJSON(record) as object }
          : null,
      });
      process.stdout.write(line + "\n");
      break;
    }

    case "info": {
      const line = JSON.stringify({ type: "info", ...toJSON(body) as object });
      process.stderr.write(`Info: ${line}\n`);
      break;
    }

    default: {
      const line = JSON.stringify({ type, ...toJSON(body) as object });
      process.stdout.write(line + "\n");
    }
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
